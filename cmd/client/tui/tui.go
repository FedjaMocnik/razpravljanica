package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	odjemalec "github.com/FedjaMocnik/razpravljalnica/internal/client"
	pb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
)

// AppState hrani stanje aplikacije.
type AppState struct {
	UserID       int64
	UserName     string
	TopicID      int64
	CurrentTopic string
	Messages     []*pb.Message
	CancelSubFn  context.CancelFunc

	SubscribedTopics map[int64]bool
	AllTopics        []*pb.Topic
}

func main() {
	app := tview.NewApplication()
	ctx := context.Background()
	state := &AppState{
		SubscribedTopics: make(map[int64]bool),
	}

	// Seznam tem (levi stolpec).
	topicsList := tview.NewList()
	topicsList.ShowSecondaryText(false)
	topicsList.SetBorder(true)
	topicsList.SetTitle("Teme (Ctrl+t)")

	// Seznam sporočil (osrednji del) - omogoča izbiro za lajkanje.
	messagesList := tview.NewList()
	messagesList.SetBorder(true)
	messagesList.SetTitle("Sporočila (Ctrl+l za fokus, Enter za lajk)")
	messagesList.SetSelectedFunc(func(i int, mainText string, secondaryText string, shortcut rune) {
		// Ob pritisku Enter na sporočilo ga "všečkamo".
		if state.TopicID == 0 || state.UserID == 0 {
			return
		}
		// Iz mainText/secondaryText ali state.Messages moramo dobiti ID.
		// Ker List hranimo sinhrono s state.Messages, uporabimo indeks `i`.
		// Vendar pozor: če je seznam prazen ali filtriran.
		// Najbolje je, da predpostavimo sinhronost z loadMessages.
		if i >= 0 && i < len(state.Messages) {
			// msg := state.Messages[i]
			// Asinhrono lajkaj, da ne blokiramo UI
			go func() {
				// Ustvarimo nov context za lajk
				_, err := func() (*pb.Message, error) {
					// Client mora biti dosegljiv. Uporabimo globalno spremenljivko ali closure.
					// Tu bomo potrebovali dostop do clienta. Rešimo kasneje v definiciji.
					return nil, fmt.Errorf("client ni dosegljiv v tem scope-u") // Placeholder
				}()
				_ = err
			}()
		}
	})

	// Globalni dnevnik (spodaj desno) - prikazuje dogodke iz vseh tem.
	logView := tview.NewTextView()
	logView.SetBorder(true)
	logView.SetTitle("Naročnine")
	logView.SetDynamicColors(true)
	logView.SetScrollable(true)
	logView.SetChangedFunc(func() {
		app.Draw()
	})

	// Vnosno polje za nova sporočila.
	messageInput := tview.NewInputField()
	messageInput.SetLabel("Sporočilo: ")
	messageInput.SetFieldWidth(0)
	messageInput.SetBorder(true)
	messageInput.SetTitle("Novo sporočilo (Ctrl+m)")

	// Pomoč uporabniku.
	helpView := tview.NewTextView()
	helpView.SetBorder(true)
	helpView.SetTitle("Pomoč")
	helpView.SetText("Ctrl+c: Izhod | Ctrl+t: Teme | Ctrl+l: Sporočila | Ctrl+m: Vnos | Ctrl+r: Osveži | Ctrl+s: Naroči/Odnaroči | Enter: Pošlji/Lajkaj")
	helpView.SetTextColor(tcell.ColorGreen)

	// Razporeditev (Layout).
	// Desna stran: Sporočila (zgoraj), Vnos (sredina), Log (spodaj).
	rightFlex := tview.NewFlex().SetDirection(tview.FlexRow)
	rightFlex.AddItem(messagesList, 0, 4, false)
	rightFlex.AddItem(messageInput, 3, 0, false)
	rightFlex.AddItem(logView, 0, 2, false)
	rightFlex.AddItem(helpView, 3, 0, false)

	root := tview.NewFlex()
	root.AddItem(topicsList, 30, 0, true)
	root.AddItem(rightFlex, 0, 1, false)

	// Povezava na strežnik (Control Plane).
	client, err := odjemalec.Povezi("localhost:9876")
	if err != nil {
		// Če ne uspe, poskusimo še direkten dostop ali panic.
		panic(fmt.Errorf("povezava ni uspela: %v", err))
	}
	defer client.Zapri()

	// Implementacija lajkanja znotraj closure-a, kjer imamo dostop do clienta.
	messagesList.SetSelectedFunc(func(i int, main string, sec string, char rune) {
		if state.TopicID == 0 || state.UserID == 0 {
			return
		}
		if i >= 0 && i < len(state.Messages) {
			msg := state.Messages[i]
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if _, err := client.LikeMessage(ctx, state.TopicID, state.UserID, msg.Id); err != nil {
					app.QueueUpdateDraw(func() {
						fmt.Fprintf(logView, "[red]Napaka pri všečkanju: %v[-]\n", err)
					})
				} else {
					// Po uspešnem lajku osvežimo sporočila
					// (Opomba: To bi lahko naredili bolj učinkovito brez ponovnega loadanja vsega)
					app.QueueUpdateDraw(func() {
						loadMessages(app, client, ctx, state, messagesList)
					})
				}
			}()
		}
	})

	// Funkcija za zagon globalne naročnine (naroči se na vse podane teme).
	startGlobalSubscription := func(topicIDs []int64) {
		// Prekliči prejšnjo naročnino, če obstaja.
		if state.CancelSubFn != nil {
			state.CancelSubFn()
		}

		subCtx, cancel := context.WithCancel(context.Background())
		state.CancelSubFn = cancel

		go func() {
			defer cancel()
			if len(topicIDs) == 0 {
				return
			}
			stream, err := client.Subscribe(subCtx, state.UserID, topicIDs, 0)
			if err != nil {
				app.QueueUpdateDraw(func() {
					fmt.Fprintf(logView, "[red]Napaka naročnine: %v[-]\n", err)
				})
				return
			}
			app.QueueUpdateDraw(func() {
				fmt.Fprintln(logView, "[green]Naročen na posodobitve vseh tem.[-]")
			})

			for {
				ev, err := stream.Recv()
				if err != nil {
					// Če je context cancelled, je to normalno.
					if subCtx.Err() == nil {
						app.QueueUpdateDraw(func() {
							fmt.Fprintf(logView, "[red]Tok zaprt: %v[-]\n", err)
						})
					}
					return
				}
				// Izpiši dogodek v log.
				msg := ev.GetMessage()
				op := ev.GetOp()
				app.QueueUpdateDraw(func() {
					// TODO: Dodaj timestamp?
					prefix := ""
					switch op {
					case pb.OpType_OP_POST:
						prefix = "NOVO"
					case pb.OpType_OP_UPDATE:
						prefix = "UREJANJE"
					case pb.OpType_OP_LIKE:
						prefix = "VŠEČEK"
					case pb.OpType_OP_DELETE:
						prefix = "IZBRIS"
					}
					fmt.Fprintf(logView, "[yellow]%s (t:%d):[-] %s\n", prefix, msg.TopicId, msg.Text)

					// Če gledamo to temo, osveži prikaz sporočil
					if state.TopicID == msg.TopicId {
						loadMessages(app, client, ctx, state, messagesList)
					}
				})
			}
		}()
	}

	// Osveži prikaz seznama tem (in označi naročene).
	refreshTopicListUI := func() {
		idx := topicsList.GetCurrentItem()
		topicsList.Clear()

		for _, t := range state.AllTopics {
			id := t.Id
			name := t.Name

			// Prikaz zvezdice, če smo naročeni
			prefix := ""
			if state.SubscribedTopics[id] {
				prefix = "*"
			}

			display := fmt.Sprintf("%s%d: %s", prefix, id, name)

			topicsList.AddItem(
				display,
				"",
				0,
				func() {
					state.TopicID = id
					state.CurrentTopic = name
					messagesList.SetTitle(fmt.Sprintf("Sporočila: %s (Enter za lajk)", name))
					loadMessages(app, client, ctx, state, messagesList)
					app.SetFocus(messageInput)
				},
			)
		}

		// Poskusimo ohraniti selekcijo
		if idx >= 0 && idx < topicsList.GetItemCount() {
			topicsList.SetCurrentItem(idx)
		}
	}

	// Nalaganje tem in zagon naročnine.
	loadTopicsAndSubscribe := func() {
		resp, err := client.ListTopics(ctx)
		if err != nil {
			fmt.Fprintln(logView, "[red]Napaka pri nalaganju tem:", err)
			return
		}

		state.AllTopics = resp.Topics
		refreshTopicListUI()
		// Tu ne zaženemo avtomatsko vseh naročnin, ohranimo le obstoječe, če so.
		// (Ob prvem zagonu je SubscribedTopics prazen)
	}

	// Logic za Ctrl+S na seznamu tem
	topicsList.SetInputCapture(func(ev *tcell.EventKey) *tcell.EventKey {
		if ev.Key() == tcell.KeyCtrlS {
			idx := topicsList.GetCurrentItem()
			if idx >= 0 && idx < len(state.AllTopics) {
				t := state.AllTopics[idx]
				if state.SubscribedTopics[t.Id] {
					delete(state.SubscribedTopics, t.Id)
				} else {
					state.SubscribedTopics[t.Id] = true
				}
				refreshTopicListUI()

				// Posodobi subscription workerja
				var ids []int64
				for id := range state.SubscribedTopics {
					ids = append(ids, id)
				}
				startGlobalSubscription(ids)
			}
			return nil
		}
		return ev
	})

	// Pošiljanje sporočila.
	sendMessage := func() {
		text := messageInput.GetText()
		if text == "" || state.TopicID == 0 {
			return
		}

		go func() {
			_, err := client.PostMessage(context.Background(), state.TopicID, state.UserID, text)
			app.QueueUpdateDraw(func() {
				if err != nil {
					fmt.Fprintf(logView, "[red]Napaka pri pošiljanju: %v[-]\n", err)
				} else {
					messageInput.SetText("")
					// Osvežitev se bo zgodila preko subscriptiona (logika v startGlobalSubscription),
					// ampak za boljšo odzivnost lahko osvežimo tudi takoj.
					loadMessages(app, client, ctx, state, messagesList)
				}
			})
		}()
	}

	// SetDoneFunc za input field (Enter).
	messageInput.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			sendMessage()
		}
	})

	// Login prompt.
	promptUsername(app, client, state, func() {
		loadTopicsAndSubscribe()
		app.SetRoot(root, true)
		app.SetFocus(topicsList)
	})

	// Globalne tipke.
	app.SetInputCapture(func(ev *tcell.EventKey) *tcell.EventKey {
		// Če smo v inputu, Esc vrne na teme.
		if messageInput.HasFocus() && ev.Key() == tcell.KeyEsc {
			app.SetFocus(topicsList)
			return nil
		}

		switch ev.Key() {
		case tcell.KeyCtrlC:
			app.Stop()
			return nil
		case tcell.KeyCtrlT:
			app.SetFocus(topicsList)
			return nil
		case tcell.KeyCtrlM:
			app.SetFocus(messageInput)
			return nil
		case tcell.KeyCtrlL:
			app.SetFocus(messagesList)
			return nil
		case tcell.KeyCtrlR:
			loadTopicsAndSubscribe()
			return nil
		}
		return ev
	})

	if err := app.Run(); err != nil {
		panic(err)
	}
}

// Pomožna funkcija za nalaganje sporočil v seznam.
func loadMessages(
	app *tview.Application,
	client *odjemalec.Odjemalec,
	ctx context.Context,
	state *AppState,
	list *tview.List,
) {
	// Naredi to asinhrono? Raje ne, ker UI thread.
	// Ampak GetMessages je mrežni klic. Moral bi biti v goroutini.
	// Zaradi poenostavitve v TUI knjižnici pogosto blokiramo Draw, če ni gorutine.
	// Tu bomo blokirali kratek čas, ali pa uporabljamo goroutine + QueueUpdateDraw.

	// Izbrišemo UI takoj, da uporabnik vidi odziv (opcijsko, lahko utripne).
	// Raje naložimo v ozadju in ko imamo podatke, posodobimo UI.

	go func() {
		resp, err := client.GetMessages(ctx, state.TopicID, 0, 50) // limit 50

		app.QueueUpdateDraw(func() {
			if err != nil {
				return
			}

			// Shranimo trenutno izbiro (indeks), da poskusimo ohraniti pozicijo.
			curIdx := list.GetCurrentItem()

			list.Clear()
			state.Messages = resp.Messages // Posodobimo lokalni state

			for _, m := range resp.Messages {
				mainText := m.Text
				secText := fmt.Sprintf("ID: %d | Avtor: %d | Lajkov: %d", m.Id, m.UserId, m.Likes)
				list.AddItem(mainText, secText, 0, nil)
			}

			// Poskusimo obnoviti indeks, če je smiselno.
			if curIdx >= 0 && curIdx < list.GetItemCount() {
				list.SetCurrentItem(curIdx)
			} else if list.GetItemCount() > 0 {
				list.SetCurrentItem(list.GetItemCount() - 1) // skoči na zadnje
			}
		})
	}()
}

// Prikaz okna za prijavo.
func promptUsername(
	app *tview.Application,
	client *odjemalec.Odjemalec,
	state *AppState,
	done func(),
) {
	input := tview.NewInputField()
	input.SetLabel("Uporabniško ime: ")
	input.SetFieldWidth(20)

	form := tview.NewForm()
	form.AddFormItem(input)
	form.AddButton("Prijava", func() {
		name := input.GetText()
		if name == "" {
			return
		}
		// Ustvarimo uporabnika, da dobimo ID.
		user, err := client.CreateUser(context.Background(), name)
		if err != nil {
			panic(err)
		}
		state.UserID = user.Id
		state.UserName = user.Name
		done()
	})

	form.SetBorder(true)
	form.SetTitle("Prijava")
	app.SetRoot(form, true)
}
