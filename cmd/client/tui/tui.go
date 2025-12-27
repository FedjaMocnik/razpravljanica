package main

import (
	"context"
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	odjemalec "github.com/FedjaMocnik/razpravljalnica/internal/client"
	pb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
)

type AppState struct {
	UserID      int64
	UserName    string
	TopicID     int64
	Messages    []*pb.Message
	CancelSubFn context.CancelFunc
}

func main() {
	app := tview.NewApplication()
	ctx := context.Background()
	state := &AppState{}

	// Tview boilerplate koda.
	topics := tview.NewList()
	topics.SetBorder(true)
	topics.SetTitle("Topics")

	messages := tview.NewTextView()
	messages.SetBorder(true)
	messages.SetTitle("Messages")
	messages.SetDynamicColors(true)

	subscribeBox := tview.NewTextView()
	subscribeBox.SetBorder(true)
	subscribeBox.SetTitle("Live subscription")
	subscribeBox.SetDynamicColors(true)

	messageInput := tview.NewInputField()
	messageInput.SetLabel("Message: ")
	messageInput.SetFieldWidth(0)
	messageInput.SetBorder(true)
	messageInput.SetTitle("New Message")

	// Layout TUI.
	right := tview.NewFlex()
	right.SetDirection(tview.FlexRow)
	right.AddItem(messages, 0, 3, false)
	right.AddItem(messageInput, 3, 1, true)
	right.AddItem(subscribeBox, 0, 2, false)

	root := tview.NewFlex()
	root.AddItem(topics, 30, 1, true)
	root.AddItem(right, 0, 3, false)

	// Povezi client na server.
	// TODO: Naredi logiko z več serverji.
	client, err := odjemalec.Povezi("localhost:9876")
	if err != nil {
		panic(err)
	}
	defer client.Zapri()

	// Pošiljanje sporočil.
	sendMessage := func() {
		text := messageInput.GetText()
		if text == "" || state.TopicID == 0 {
			return
		}

		if state.UserID == 0 {
			user, err := client.CreateUser(context.Background(), "anonymous")
			if err != nil {
				return
			}
			state.UserID = user.Id
			state.UserName = user.Name
		}

		_, err := client.PostMessage(
			context.Background(),
			state.TopicID,
			state.UserID,
			text,
		)
		if err != nil {
			fmt.Fprintln(messages, "[red]Send failed:", err)
			return
		}

		messageInput.SetText("")
		state.Messages = loadMessages(client, ctx, state.TopicID, messages)
	}

	messageInput.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			sendMessage()
		}
	})

	// Load topics: Trenutno lazy.
	loadTopics := func() {
		resp, err := client.ListTopics(ctx)
		if err != nil {
			fmt.Fprintln(messages, "[red]Napaka:", err)
			return
		}

		topics.Clear()

		for _, t := range resp.Topics {
			id := t.Id
			name := t.Name

			topics.AddItem(
				fmt.Sprintf("%d: %s", id, name),
				"",
				0,
				func() {
					state.TopicID = id
					state.Messages = loadMessages(client, ctx, id, messages)
					startSubscription(app, client, state, subscribeBox)
					app.SetFocus(messageInput)
				},
			)
		}
	}

	// Login prompt drži trenutnega userja.
	promptUsername(app, client, state, func() {
		loadTopics()
		app.SetRoot(root, true)
		app.SetFocus(topics)
	})

	// Input capture in kontroliranje fokusa.
	app.SetInputCapture(func(ev *tcell.EventKey) *tcell.EventKey {
		if messageInput.HasFocus() {
			if ev.Key() == tcell.KeyEsc {
				app.SetFocus(topics)
				return nil
			}
			return ev
		}

		switch ev.Key() {
		case tcell.KeyCtrlC:
			app.Stop()
			return nil

		case tcell.KeyCtrlT:
			app.SetFocus(topics)
			return nil

		case tcell.KeyCtrlM:
			app.SetFocus(messageInput)
			return nil

		case tcell.KeyCtrlR:
			loadTopics()
			return nil

		case tcell.KeyCtrlL:
			if state.TopicID == 0 || len(state.Messages) == 0 {
				return nil
			}
			last := state.Messages[len(state.Messages)-1]
			client.LikeMessage(ctx, state.TopicID, state.UserID, last.Id)
			state.Messages = loadMessages(client, ctx, state.TopicID, messages)
			return nil
		}

		return ev
	})

	if err := app.Run(); err != nil {
		panic(err)
	}
}

// Pomozne funkcije. Večinoma interface s serverjem.
func promptUsername(
	app *tview.Application,
	client *odjemalec.Odjemalec,
	state *AppState,
	done func(),
) {
	input := tview.NewInputField()
	input.SetLabel("Username: ")
	input.SetFieldWidth(20)

	form := tview.NewForm()
	form.AddFormItem(input)
	form.AddButton("Login", func() {
		name := input.GetText()
		if name == "" {
			return
		}
		user, err := client.CreateUser(context.Background(), name)
		if err != nil {
			panic(err)
		}
		state.UserID = user.Id
		state.UserName = user.Name
		done()
	})

	form.SetBorder(true)
	form.SetTitle("Login")
	app.SetRoot(form, true)
}

func loadMessages(
	client *odjemalec.Odjemalec,
	ctx context.Context,
	topicID int64,
	view *tview.TextView,
) []*pb.Message {
	view.Clear()

	resp, err := client.GetMessages(ctx, topicID, 0, 50)
	if err != nil {
		fmt.Fprintln(view, "[red]Napaka:", err)
		return nil
	}

	for _, m := range resp.Messages {
		fmt.Fprintf(
			view,
			"[yellow]%d[/] (%d) %d: %s\n",
			m.Id,
			m.UserId,
			m.Likes,
			m.Text,
		)
	}

	return resp.Messages
}

func startSubscription(
	app *tview.Application,
	client *odjemalec.Odjemalec,
	state *AppState,
	view *tview.TextView,
) {
	if state.CancelSubFn != nil {
		state.CancelSubFn()
	}

	view.Clear()

	ctx, cancel := context.WithCancel(context.Background())
	state.CancelSubFn = cancel

	go func() {
		stream, err := client.Subscribe(
			ctx,
			state.UserID,
			[]int64{state.TopicID},
			0,
		)
		if err != nil {
			app.QueueUpdateDraw(func() {
				fmt.Fprintln(view, "[red]Subscribe error:", err)
			})
			return
		}

		for {
			msg, err := stream.Recv()
			if err != nil {
				app.QueueUpdateDraw(func() {
					fmt.Fprintln(view, "[red]Stream closed:", err)
				})
				return
			}

			app.QueueUpdateDraw(func() {
				fmt.Fprintf(
					view,
					"[green]LIVE %d[/]: %s\n",
					msg.Message.Id,
					msg.Message.Text,
				)
			})
		}
	}()
}
