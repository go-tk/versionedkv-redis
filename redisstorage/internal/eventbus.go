package internal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

type EventBus struct {
	pubSub  *redis.PubSub
	options EventBusOptions

	mu                    sync.Mutex
	subscriptions         map[string]*subscription
	works                 chan func()
	idleSubscriptionCount int
	isClosed1             int32
}

type EventBusOptions struct {
	ChannelNamePrefix            string
	MaxNumberOfIdleSubscriptions int
	IdleSubscriptionTimeout      time.Duration
	Go                           func(func())
}

func (ebo *EventBusOptions) sanitize() {
	if ebo.ChannelNamePrefix == "" {
		ebo.ChannelNamePrefix = "event:"
	}
	if ebo.MaxNumberOfIdleSubscriptions < 1 {
		ebo.MaxNumberOfIdleSubscriptions = 100
	}
	if ebo.IdleSubscriptionTimeout < 1 {
		ebo.IdleSubscriptionTimeout = 100 * time.Second
	}
	if ebo.Go == nil {
		ebo.Go = func(routine func()) { go routine() }
	}
}

func (eb *EventBus) Init(pubSub *redis.PubSub, options EventBusOptions) *EventBus {
	eb.pubSub = pubSub
	eb.options = options
	eb.options.sanitize()
	eb.works = make(chan func(), 100)
	eb.options.Go(eb.handleMessages)
	eb.options.Go(eb.doWorks)
	return eb
}

func (eb *EventBus) handleMessages() {
	for {
		if eb.IsClosed() {
			return
		}
		message, err := eb.pubSub.Receive(context.Background())
		if err != nil {
			continue
		}
		switch message := message.(type) {
		case *redis.Message:
			watchers, err := eb.removeWatchers(message.Channel)
			if err != nil {
				if err != ErrEventBusClosed {
					panic("unreachable")
				}
				return
			}
			var eventArgs EventArgs
			if n := len(message.PayloadSlice); n == 0 {
				eventArgs.Message = message.Payload
			} else {
				eventArgs.Message = message.PayloadSlice[n-1]
			}
			for watcher := range watchers {
				watcher.FireEvent(eventArgs)
			}
		case *redis.Subscription:
			watchers, err := eb.removeWatchers(message.Channel)
			if err != nil {
				if err != ErrEventBusClosed {
					panic("unreachable")
				}
				return
			}
			eventArgs := EventArgs{
				WatchLoss: true,
			}
			for watcher := range watchers {
				watcher.FireEvent(eventArgs)
			}
		}
	}
}

func (eb *EventBus) removeWatchers(channelName string) (map[*watcher]struct{}, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if eb.IsClosed() {
		return nil, ErrEventBusClosed
	}
	subscription, ok := eb.subscriptions[channelName]
	if !ok {
		return nil, nil
	}
	if watchers := subscription.Watchers; watchers != nil {
		eb.freeSubscription(subscription)
		return watchers, nil
	}
	delete(eb.subscriptions, channelName)
	if len(eb.subscriptions) == 0 {
		eb.subscriptions = nil
	}
	eb.idleSubscriptionCount -= 1
	eb.works <- func() { eb.pubSub.Unsubscribe(context.Background(), channelName) }
	return nil, nil
}

func (eb *EventBus) doWorks() {
	for work := range eb.works {
		if eb.IsClosed() {
			return
		}
		work()
	}
}

func (eb *EventBus) AddWatcher(eventName string) (Watcher, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if eb.IsClosed() {
		return Watcher{}, ErrEventBusClosed
	}
	channelName := eb.eventNameToChannelName(eventName)
	subscription, ok := eb.subscriptions[channelName]
	if !ok {
		subscription = eb.createSubscription(channelName)
	}
	if subscription.Watchers == nil {
		subscription.Watchers = make(map[*watcher]struct{})
		if !subscription.Deadline.IsZero() {
			subscription.Deadline = time.Time{}
			eb.idleSubscriptionCount--
		}
	}
	watcher := new(watcher).Init()
	subscription.Watchers[watcher] = struct{}{}
	wrappedWatcher := Watcher{watcher}
	return wrappedWatcher, nil
}

func (eb *EventBus) createSubscription(channelName string) *subscription {
	if eb.subscriptions == nil {
		eb.subscriptions = make(map[string]*subscription)
	}
	var subscription subscription
	eb.subscriptions[channelName] = &subscription
	eb.works <- func() { eb.pubSub.Subscribe(context.Background(), channelName) }
	return &subscription
}

func (eb *EventBus) RemoveWatcher(eventName string, wrappedWatcher Watcher) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if eb.IsClosed() {
		return ErrEventBusClosed
	}
	channelName := eb.eventNameToChannelName(eventName)
	subscription, ok := eb.subscriptions[channelName]
	if !ok {
		return nil
	}
	watcher := wrappedWatcher.w
	if _, ok := subscription.Watchers[watcher]; !ok {
		return nil
	}
	delete(subscription.Watchers, watcher)
	if len(subscription.Watchers) >= 1 {
		return nil
	}
	eb.freeSubscription(subscription)
	return nil
}

func (eb *EventBus) Close() error {
	if atomic.SwapInt32(&eb.isClosed1, 1) != 0 {
		return ErrEventBusClosed
	}
	err := eb.pubSub.Close()
	eb.mu.Lock()
	defer eb.mu.Unlock()
	close(eb.works)
	return err
}

func (eb *EventBus) ChannelNamePrefix() string { return eb.options.ChannelNamePrefix }

func (eb *EventBus) IsClosed() bool {
	return atomic.LoadInt32(&eb.isClosed1) != 0
}

func (eb *EventBus) eventNameToChannelName(eventName string) string {
	return eb.options.ChannelNamePrefix + eventName
}

func (eb *EventBus) freeSubscription(subscription *subscription) {
	now := time.Now()
	if eb.idleSubscriptionCount == eb.options.MaxNumberOfIdleSubscriptions {
		eb.deleteSomeIdleSubscriptions(now)
	}
	subscription.Watchers = nil
	subscription.Deadline = now.Add(eb.options.IdleSubscriptionTimeout)
	eb.idleSubscriptionCount++
}

func (eb *EventBus) deleteSomeIdleSubscriptions(now time.Time) {
	var channelNames []string
	for channelName, subscription := range eb.subscriptions {
		if subscription.Deadline.IsZero() {
			continue
		}
		if subscription.Deadline.After(now) {
			continue
		}
		channelNames = append(channelNames, channelName)
	}
	if channelNames == nil {
		for channelName, subscription := range eb.subscriptions {
			if subscription.Deadline.IsZero() {
				continue
			}
			channelNames = []string{channelName}
			break
		}
	}
	for _, channelName := range channelNames {
		delete(eb.subscriptions, channelName)
	}
	eb.idleSubscriptionCount -= len(channelNames)
	eb.works <- func() { eb.pubSub.Unsubscribe(context.Background(), channelNames...) }
}

type Watcher struct{ w *watcher }

func (w Watcher) Event() <-chan struct{} { return w.w.Event() }
func (w Watcher) EventArgs() EventArgs   { return w.w.EventArgs() }

type EventArgs struct {
	WatchLoss bool
	Message   string
}

var ErrEventBusClosed error = errors.New("internal: event bus closed")

type subscription struct {
	Watchers map[*watcher]struct{}
	Deadline time.Time
}

type watcher struct {
	event     chan struct{}
	eventArgs EventArgs
}

func (w *watcher) Init() *watcher {
	w.event = make(chan struct{})
	return w
}

func (w *watcher) FireEvent(eventArgs EventArgs) {
	w.eventArgs = eventArgs
	close(w.event)
}

func (w *watcher) Event() <-chan struct{} {
	return w.event
}

func (w *watcher) EventArgs() EventArgs {
	return w.eventArgs
}
