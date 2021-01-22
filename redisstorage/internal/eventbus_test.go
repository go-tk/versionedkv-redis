package internal_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/go-tk/testcase"
	. "github.com/go-tk/versionedkv-redis/redisstorage/internal"
	"github.com/stretchr/testify/assert"
)

func TestEventBus_AddWatcher(t *testing.T) {
	type Init struct {
		Options EventBusOptions
	}
	type Input struct {
		EventName string
	}
	type Output struct {
		Err error
	}
	type State = EventBusDetails
	type Context struct {
		RC redisClient
		EB EventBus

		Init           Init
		Input          Input
		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{
			Init: Init{
				Options: EventBusOptions{
					ChannelNamePrefix:            "test:",
					MaxNumberOfIdleSubscriptions: 100,
					IdleSubscriptionTimeout:      100 * time.Second,
				},
			},
		}
	}).Setup(func(t *testing.T, c *Context) {
		rc, err := makeRedisClient()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		c.RC = rc
		pubSub := rc.Subscribe(context.Background())
		c.EB.Init(pubSub, c.Init.Options)
	}).Run(func(t *testing.T, c *Context) {
		_, err := c.EB.AddWatcher(c.Input.EventName)
		var output Output
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
		state := c.EB.Inspect()
		assert.Equal(t, c.ExpectedState, state)
	}).Teardown(func(t *testing.T, c *Context) {
		if c.ExpectedOutput.Err != ErrEventBusClosed {
			err := c.EB.Close()
			assert.NoError(t, err)
		}
		c.RC.Close()
	})
	testcase.RunListParallel(t,
		tc.Copy().
			Given("bus closed").
			Then("should fail with error ErrEventBusClosed").
			PreRun(func(t *testing.T, c *Context) {
				err := c.EB.Close()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "foo"
				c.ExpectedOutput.Err = ErrEventBusClosed
				c.ExpectedState.IsClosed = true
			}),
		tc.Copy().
			Then("should succeed (1)").
			PreRun(func(t *testing.T, c *Context) {
				createIdleSubscription(t, &c.EB, "foo")
				c.Input.EventName = "foo"
				c.ExpectedState.Subscriptions = map[string]SubscriptionDetails{
					"test:foo": {
						NumberOfWatchers: 1,
					},
				}
			}),
		tc.Copy().
			Then("should succeed (2)").
			PreRun(func(t *testing.T, c *Context) {
				createIdleSubscription(t, &c.EB, "foo")
				_, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "foo"
				c.ExpectedState.Subscriptions = map[string]SubscriptionDetails{
					"test:foo": {
						NumberOfWatchers: 2,
					},
				}
			}),
	)
}

func TestEventBus_RemoveWatcher(t *testing.T) {
	type Init struct {
		Options EventBusOptions
	}
	type Input struct {
		EventName string
		Watcher   Watcher
	}
	type Output struct {
		Err error
	}
	type State = EventBusDetails
	type Context struct {
		RC redisClient
		EB EventBus

		Init           Init
		Input          Input
		ExpectedOutput Output
		ExpectedStates []State
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{
			Init: Init{
				Options: EventBusOptions{
					ChannelNamePrefix:            "test:",
					MaxNumberOfIdleSubscriptions: 100,
					IdleSubscriptionTimeout:      100 * time.Second,
				},
			},
		}
	}).Setup(func(t *testing.T, c *Context) {
		rc, err := makeRedisClient()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		c.RC = rc
		pubSub := rc.Subscribe(context.Background())
		c.EB.Init(pubSub, c.Init.Options)
	}).Run(func(t *testing.T, c *Context) {
		err := c.EB.RemoveWatcher(c.Input.EventName, c.Input.Watcher)
		var output Output
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
		state := c.EB.Inspect()
		if c.ExpectedStates == nil {
			assert.Equal(t, c.ExpectedState, state)
		} else {
			assert.Contains(t, c.ExpectedStates, state)
		}
	}).Teardown(func(t *testing.T, c *Context) {
		if c.ExpectedOutput.Err != ErrEventBusClosed {
			err := c.EB.Close()
			assert.NoError(t, err)
		}
		c.RC.Close()
	})
	testcase.RunListParallel(t,
		tc.Copy().
			Given("bus closed").
			Then("should fail with error ErrEventBusClosed").
			PreRun(func(t *testing.T, c *Context) {
				err := c.EB.Close()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "foo"
				c.ExpectedOutput.Err = ErrEventBusClosed
				c.ExpectedState.IsClosed = true
			}),
		tc.Copy().
			When("subscription to channel of given event exists").
			Then("should succeed (1)").
			PreRun(func(t *testing.T, c *Context) {
				createIdleSubscription(t, &c.EB, "foo")
				w, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "foo"
				c.Input.Watcher = w
				c.ExpectedState.Subscriptions = map[string]SubscriptionDetails{
					"test:foo": {
						IsIdle: true,
					},
				}
				c.ExpectedState.NumberOfIdleSubscriptions = 1
			}),
		tc.Copy().
			When("subscription to channel of given event exists").
			Then("should succeed (2)").
			PreRun(func(t *testing.T, c *Context) {
				createIdleSubscription(t, &c.EB, "foo")
				w, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				_, err = c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "foo"
				c.Input.Watcher = w
				c.ExpectedState.Subscriptions = map[string]SubscriptionDetails{
					"test:foo": {
						NumberOfWatchers: 1,
					},
				}
			}),
		tc.Copy().
			When("subscription to channel of given event does not exist").
			Then("should succeed").
			PreRun(func(t *testing.T, c *Context) {
				c.Input.EventName = "foo"
			}),
		tc.Copy().
			When("given watcher has already been removed").
			Then("should succeed").
			PreRun(func(t *testing.T, c *Context) {
				w, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				err = c.EB.RemoveWatcher("foo", w)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "foo"
				c.Input.Watcher = w
				c.ExpectedState.Subscriptions = map[string]SubscriptionDetails{
					"test:foo": {
						IsIdle: true,
					},
				}
				c.ExpectedState.NumberOfIdleSubscriptions = 1
			}),
		tc.Copy().
			When("number of idle subscriptions is to reach option MaxNumberOfIdleSubscriptions").
			Then("should delete all idle subscriptions expired").
			PreSetup(func(t *testing.T, c *Context) {
				c.Init.Options.MaxNumberOfIdleSubscriptions = 3
				c.Init.Options.IdleSubscriptionTimeout = 100 * time.Millisecond
			}).
			PreRun(func(t *testing.T, c *Context) {
				createIdleSubscription(t, &c.EB, "kkk")
				_, err := c.EB.AddWatcher("kkk")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				createIdleSubscription(t, &c.EB, "1")
				createIdleSubscription(t, &c.EB, "2")
				createIdleSubscription(t, &c.EB, "3")
				time.Sleep(200 * time.Millisecond)
				w, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "foo"
				c.Input.Watcher = w
				c.ExpectedState.Subscriptions = map[string]SubscriptionDetails{
					"test:kkk": {
						NumberOfWatchers: 1,
					},
					"test:foo": {
						IsIdle: true,
					},
				}
				c.ExpectedState.NumberOfIdleSubscriptions = 1
			}),
		tc.Copy().
			When("number of idle subscriptions is to reach option MaxNumberOfIdleSubscriptions"+
				" and have no idle subscription expired").
			Then("should delete a idle subscription randomly").
			PreSetup(func(t *testing.T, c *Context) {
				c.Init.Options.MaxNumberOfIdleSubscriptions = 2
			}).
			PreRun(func(t *testing.T, c *Context) {
				createIdleSubscription(t, &c.EB, "kkk")
				_, err := c.EB.AddWatcher("kkk")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				createIdleSubscription(t, &c.EB, "foo")
				createIdleSubscription(t, &c.EB, "bar")
				w, err := c.EB.AddWatcher("baz")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "baz"
				c.Input.Watcher = w
				c.ExpectedStates = []State{
					{
						Subscriptions: map[string]SubscriptionDetails{
							"test:kkk": {
								NumberOfWatchers: 1,
							},
							"test:bar": {
								IsIdle: true,
							},
							"test:baz": {
								IsIdle: true,
							},
						},
						NumberOfIdleSubscriptions: 2,
					},
					{
						Subscriptions: map[string]SubscriptionDetails{
							"test:kkk": {
								NumberOfWatchers: 1,
							},
							"test:foo": {
								IsIdle: true,
							},
							"test:baz": {
								IsIdle: true,
							},
						},
						NumberOfIdleSubscriptions: 2,
					},
				}
			}),
	)
}

func TestEventBus_Close(t *testing.T) {
	rc, err := makeRedisClient()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer rc.Close()
	pubSub := rc.Subscribe(context.Background())
	var wg sync.WaitGroup
	eb := new(EventBus).Init(pubSub, EventBusOptions{
		Go: func(routine func()) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				routine()
			}()
		},
	})
	err = eb.Close()
	assert.NoError(t, err)
	wg.Wait()
	err = eb.Close()
	assert.Equal(t, ErrEventBusClosed, err)
}

func TestEventBus_handleMessages(t *testing.T) {
	type Init struct {
		Options EventBusOptions
	}
	type State = EventBusDetails
	type Context struct {
		RC redisClient
		EB EventBus

		Init           Init
		ExpectedStates []State
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{
			Init: Init{
				Options: EventBusOptions{
					ChannelNamePrefix:            "test:",
					MaxNumberOfIdleSubscriptions: 100,
					IdleSubscriptionTimeout:      100 * time.Second,
				},
			},
		}
	}).Setup(func(t *testing.T, c *Context) {
		rc, err := makeRedisClient()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		c.RC = rc
		pubSub := rc.Subscribe(context.Background())
		c.EB.Init(pubSub, c.Init.Options)
	}).Teardown(func(t *testing.T, c *Context) {
		state := c.EB.Inspect()
		if c.ExpectedStates == nil {
			assert.Equal(t, c.ExpectedState, state)
		} else {
			assert.Contains(t, c.ExpectedStates, state)
		}
		err := c.EB.Close()
		assert.NoError(t, err)
		c.RC.Close()
	})
	testcase.RunListParallel(t,
		tc.Copy().
			When("fire event").
			Then("should remove watchers and return corresponding event args").
			Run(func(t *testing.T, c *Context) {
				createIdleSubscription(t, &c.EB, "foo")
				w1, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w2, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				time.AfterFunc(100*time.Millisecond, func() {
					cmd := c.RC.Publish(context.Background(), c.EB.ChannelNamePrefix()+"foo", "bar")
					assert.NoError(t, cmd.Err())
				})
				select {
				case <-w1.Event():
				case <-time.After(10 * time.Second):
					t.Fatal("timed out")
				}
				ea1 := w1.EventArgs()
				assert.Equal(t, EventArgs{Message: "bar"}, ea1)
				select {
				case <-w2.Event():
				case <-time.After(10 * time.Second):
					t.Fatal("timed out")
				}
				ea2 := w2.EventArgs()
				assert.Equal(t, EventArgs{Message: "bar"}, ea2)
				c.ExpectedState.Subscriptions = map[string]SubscriptionDetails{
					"test:foo": {
						IsIdle: true,
					},
				}
				c.ExpectedState.NumberOfIdleSubscriptions = 1
			}),
		tc.Copy().
			When("fire event, channel of event is idle").
			Then("should delete subscription").
			Run(func(t *testing.T, c *Context) {
				createIdleSubscription(t, &c.EB, "foo")
				cmd := c.RC.Publish(context.Background(), c.EB.ChannelNamePrefix()+"foo", "bar")
				assert.NoError(t, cmd.Err())
				time.Sleep(100 * time.Millisecond)
			}),
		tc.Copy().
			When("fire event, number of idle subscriptions is to reach option MaxNumberOfIdleSubscriptions").
			Then("should delete all idle subscriptions expired").
			PreSetup(func(t *testing.T, c *Context) {
				c.Init.Options.MaxNumberOfIdleSubscriptions = 3
				c.Init.Options.IdleSubscriptionTimeout = 100 * time.Millisecond
			}).
			Run(func(t *testing.T, c *Context) {
				createIdleSubscription(t, &c.EB, "kkk")
				_, err := c.EB.AddWatcher("kkk")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				createIdleSubscription(t, &c.EB, "1")
				createIdleSubscription(t, &c.EB, "2")
				createIdleSubscription(t, &c.EB, "3")
				time.Sleep(200 * time.Millisecond)
				w, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				select {
				case <-w.Event():
				case <-time.After(10 * time.Second):
					t.Fatal("timed out")
				}
				c.ExpectedState.Subscriptions = map[string]SubscriptionDetails{
					"test:kkk": {
						NumberOfWatchers: 1,
					},
					"test:foo": {
						IsIdle: true,
					},
				}
				c.ExpectedState.NumberOfIdleSubscriptions = 1
			}),
		tc.Copy().
			When("fire event, number of idle subscriptions is to reach option MaxNumberOfIdleSubscriptions"+
				" and have no idle subscription expired").
			Then("should delete a idle subscription randomly").
			PreSetup(func(t *testing.T, c *Context) {
				c.Init.Options.MaxNumberOfIdleSubscriptions = 2
			}).
			Run(func(t *testing.T, c *Context) {
				createIdleSubscription(t, &c.EB, "kkk")
				_, err := c.EB.AddWatcher("kkk")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				createIdleSubscription(t, &c.EB, "foo")
				createIdleSubscription(t, &c.EB, "bar")
				w, err := c.EB.AddWatcher("baz")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				select {
				case <-w.Event():
				case <-time.After(10 * time.Second):
					t.Fatal("timed out")
				}
				c.ExpectedStates = []State{
					{
						Subscriptions: map[string]SubscriptionDetails{
							"test:kkk": {
								NumberOfWatchers: 1,
							},
							"test:bar": {
								IsIdle: true,
							},
							"test:baz": {
								IsIdle: true,
							},
						},
						NumberOfIdleSubscriptions: 2,
					},
					{
						Subscriptions: map[string]SubscriptionDetails{
							"test:kkk": {
								NumberOfWatchers: 1,
							},
							"test:foo": {
								IsIdle: true,
							},
							"test:baz": {
								IsIdle: true,
							},
						},
						NumberOfIdleSubscriptions: 2,
					},
				}
			}),
	)
}

type redisClient struct {
	*redis.Client

	m *miniredis.Miniredis
}

func makeRedisClient() (redisClient, error) {
	m, err := miniredis.Run()
	if err != nil {
		return redisClient{}, err
	}
	c := redis.NewClient(&redis.Options{Addr: m.Addr()})
	return redisClient{
		Client: c,

		m: m,
	}, nil
}

func (rc redisClient) Close() error {
	err := rc.Client.Close()
	rc.m.Close()
	return err
}

func createIdleSubscription(t *testing.T, eb *EventBus, eventName string) {
	w, err := eb.AddWatcher(eventName)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	select {
	case <-w.Event():
	case <-time.After(10 * time.Second):
		t.Fatal("timed out")
	}
	ea := w.EventArgs()
	assert.True(t, ea.WatchLoss)
}
