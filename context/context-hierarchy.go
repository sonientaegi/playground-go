package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// TEST 용 supplier, consumer 개수 설정
const NumOfSupplier = 7
const NumOfConsumer = 8

// Error 정의
var ErrSupplierOOM = errors.New("supplier OOM")
var ErrConsumerTimeout = errors.New("consumer timeout")

// Supplier 기본적으로 부모의 context를 공유하며 부모 context의 수명주기에 의존한다.
type Supplier struct {
	messages chan struct{}
	ctx      context.Context
}

func NewSupplier(ctx context.Context, ch chan struct{}) *Supplier {
	return &Supplier{
		messages: ch,
		ctx:      ctx,
	}
}

// Run 생산자의 메인 함수. 0 ~ 10초 사이 랜덤한 시간에 10%의 확률로 OOM이 발생한다.
func (s *Supplier) Run() {
	// 전달 받은 부모 context에서 필요한 자원을 꺼내 쓸 수 있다.
	chError := s.ctx.Value("error").(chan error)
	wg := &sync.WaitGroup{}

	// OOM을 묘사하기위해 만든 더미 context
	ctxDummy, cancel := context.WithCancel(s.ctx)

	// 10% 확율로 OOM을 발생시킨다.
	wg.Add(1)
	go func() {
		defer wg.Done()

		timer := time.NewTimer(0)
		// defer timer.Stop() // Not necessary since 1.23

		for {
			// 0 ~ 10 초 랜덤한 시간에...
			timer.Reset(time.Duration(rand.Float32()*10000) * time.Millisecond)
			select {
			case <-timer.C:
				// 10%의 확율로 OOM 발생.
				if rand.Float32() <= 0.1 {
					cancel()
					return
				}
			case <-s.ctx.Done():
				// 부모 수명주기에 따름.
				return
			}
		}
	}()

	// 생산자 메인루프. 1초에 한번 생산한다.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
MAIN:
	for {
		select {
		case <-ticker.C:
			fmt.Println("supply")
			s.messages <- struct{}{}
		case <-ctxDummy.Done():
			// OOM 발생
			// callback 함수는 동시성문제가 발생할 수 있다. 따라서 channel을 이용한 오류전파를 권장함.
			chError <- ErrSupplierOOM

			// 죽었으니까 바로 종료
			return
		case <-s.ctx.Done():
			break MAIN // go 에서는 loop break를 권장한다.
		}
	}
	wg.Wait()
	fmt.Println("supplier terminated")
}

// Consumer 기본적으로 부모의 context를 공유하며 부모 context의 수명주기에 의존한다.
type Consumer struct {
	ch      chan struct{}
	ctx     context.Context
	chError chan error
}

// Run 소비자의 메인함수. 읽기 요청마다 timeout을 가진다. 매 초당 5%의 확율로 타임아웃이 발생한다.
func (s *Consumer) Run() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
MAIN:
	for {
		ctx, cancel := context.WithTimeout(s.ctx, time.Duration(0.95+rand.Float32())*time.Second)
		select {
		case <-ticker.C:
			cancel()
			if _, isFilled := <-s.ch; isFilled {
				fmt.Println("consume")
			}
		case <-ctx.Done():
			cancel()

			// 만약 종료 사유가 타임아웃이라면...
			if ctx.Err() == context.DeadlineExceeded {
				// callback 함수는 동시성문제가 발생할 수 있다. 따라서 channel을 이용한 오류전파를 권장함.
				s.chError <- ErrConsumerTimeout

				// 테스트를 위해 바로 죽인다.
				return
			}

			// 부모 수명주기에 따름.
			break MAIN
		}
	}

	fmt.Println("consumer terminated")
}

func NewConsumer(ctx context.Context, ch chan struct{}, chError chan error) *Consumer {
	return &Consumer{
		ch:      ch,
		ctx:     ctx,
		chError: chError,
	}
}

// Manager 생산자와 소비자를 관리하고 모니터링한다.
type Manager struct {
	wgSupplier *sync.WaitGroup
	wgConsumer *sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	messages   chan struct{}
	chError    chan error
}

func NewManager(ctxParent context.Context) *Manager {
	chError := make(chan error, NumOfSupplier)
	ctx, cancel := context.WithCancel(context.WithValue(ctxParent, "error", chError))
	return &Manager{
		wgSupplier: &sync.WaitGroup{},
		wgConsumer: &sync.WaitGroup{},
		ctx:        ctx,
		cancel:     cancel,
		messages:   make(chan struct{}, NumOfSupplier*2),
		chError:    chError,
	}
}

// Run 매니저의 메인함수. 5%의 확율로 크리티컬한 장애가 발생한다.
// 생산자와 소비자를 생성하고 작업을 지시한다. 생산자나 소비자에 장애가 생기면 장애를 확인하고 생산자 또는 소비자를 새로 투입한다.
// backpressure를 모니터링하며, 이를 응용하여 동적으로 부하분산을 구현할 수도 있다.
func (s *Manager) Run() (reason error) {
	// 생산자 투입
	for i := 0; i < NumOfSupplier; i++ {
		s.launchSupplier()
	}

	// 소비자 투입
	for i := 0; i < NumOfConsumer; i++ {
		s.launchConsumer()
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
MAIN:
	for {
		select {
		// 부모 수명주기 따름.
		case <-s.ctx.Done():
			break MAIN

		// 오류 전파시 생산자, 소비자를 재시작한다.
		case err := <-s.chError:
			fmt.Println(err)
			if errors.Is(err, ErrSupplierOOM) {
				s.launchSupplier()
			} else if errors.Is(err, ErrConsumerTimeout) {
				s.launchConsumer()
			} else {
				// 그 외의 오류는 그냥 죽이자.
				s.cancel()
				reason = err
				break MAIN
			}
		// 백프래셔 모니터링
		case <-ticker.C:
			fmt.Println("remains", len(s.messages))

			// 5%의 확율로 죽여버리자.
			if rand.Float32() > 0.95 {
				s.cancel()
				reason = errors.New("some fatal error")
				break MAIN
			}
		}
	}

	// 생산자가 다 종료하길 기다린다.
	s.wgSupplier.Wait()

	// 채널을 닫는다. 채널을 닫아도 읽기는 가능하다.
	// 소비자가 생산자보다 많은경우 채널이 비어 데드락에 걸릴 수 있음.
	close(s.messages)

	// 소비자가 다 종료하길 기다린다.
	s.wgConsumer.Wait()

	return
}

func (s *Manager) launchSupplier() {
	go func() {
		s.wgSupplier.Add(1)
		defer s.wgSupplier.Done()
		NewSupplier(s.ctx, s.messages).Run()
	}()
}

func (s *Manager) launchConsumer() {
	go func() {
		s.wgConsumer.Add(1)
		defer s.wgConsumer.Done()
		NewConsumer(s.ctx, s.messages, s.chError).Run()
	}()
}

func main() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sig
		cancel()
	}()

	if err := NewManager(ctx).Run(); err != nil {
		fmt.Println("accidentally terminated by", err)
	} else {
		fmt.Println("gracefully shutdown")
	}
}
