package tumaikin.research.Reactore.Core;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import reactor.core.publisher.*;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.function.Predicate;

@RunWith(SpringRunner.class)
@SpringBootTest
public class Tests {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(Tests.class);

    //consumer didnt get records, which was emited before his subscribing
    @Test
    public void HotPublisher() throws InterruptedException {
        DirectProcessor<String> hotSource = DirectProcessor.create();

        Flux<String> hotFlux = hotSource.map(String::toUpperCase);
        hotSource.onNext("red");
        hotSource.onNext("black");

        hotFlux.subscribe(d -> log.info("Consumer 1 read Hot Source: "+d));

        hotSource.onNext("blue");
        hotSource.onNext("green");

        hotFlux.subscribe(d -> log.info("Consumer 2 read Hot Source: "+d));

        hotSource.onNext("orange");
        hotSource.onNext("purple");
        hotSource.onComplete();
    }

    //.subscribe(Subscriber<T>)
    @Test
    public void subscriberInterface() {
        Flux.just(1, 2, 3, 4)
                .subscribe(new Subscriber<Integer>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        log.info("onSubscribe");
                        s.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onNext(Integer integer) {
                        log.info("onNext");
                        log.info("i = " + integer);
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.info("onError");
                    }

                    @Override
                    public void onComplete() {
                        log.info("onComplete");
                    }
                });
    }

    //BaseSubscriber
    @Test
    public void baseSubscriber() {
        Flux.range(1, 7)
                .log()
                .subscribe(new BaseSubscriber<Integer>() {
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        request(1);
                    }

                    @Override
                    protected void hookOnNext(Integer value) {
                        //request(1);
                        super.hookOnNext(value);
                    }

                    @Override
                    protected void hookOnComplete() {
                        super.hookOnComplete();
                    }

                    @Override
                    protected void hookOnError(Throwable throwable) {
                        super.hookOnError(throwable);
                    }

                    @Override
                    protected void hookOnCancel() {
                        super.hookOnCancel();
                    }

                    @Override
                    protected void hookFinally(SignalType type) {
                        super.hookFinally(type);
                    }

                });
    }


    //get 5 events for a iteration
    @Test
    public void simpleBackPressure() {
        Predicate<Integer> emitTrigger = (count) -> count % 5 == 0;
        Flux.range(1, 12)
                .log()
                .subscribe(new Subscriber<Integer>() {
                    private Subscription s;
                    int messageCount;

                    @Override
                    public void onSubscribe(Subscription s) {
                        this.s = s;
                        s.request(5);
                    }

                    @Override
                    public void onNext(Integer integer) {
                        messageCount++;
                        if (emitTrigger.test(messageCount)) {
                            s.request(5);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {}

                    @Override
                    public void onComplete() {
                        log.info("processed message = " + messageCount);
                    }
                });
    }

    @Test
    //SynchronousSink
    public void synchronousSink() {
        Flux<String> flux = Flux.generate(
                () -> 0,
                (state, sink) -> {
                    sink.next("3 x " + state + " = " + 3*state);
                    if (state == 10) sink.complete();
                    return state + 1;
                });
        flux.log()
                .subscribe();
    }

    @Test
    //not matter in which tread create Mono/Flux, its proceed in subscribe thread
    public void thread1() throws InterruptedException {
        log.info(Thread.currentThread().getName());
        final Mono<String> mono = Mono.just("hello ");

        new Thread(() -> mono
                .map(msg ->
                {
                    log.info("in map ->" + Thread.currentThread().getName());
                    return msg + "thread ";
                })
                .subscribe(v ->
                        log.info(v + Thread.currentThread().getName())
                )
        ).start();
        Thread.sleep(1000);
    }

    @Test

    //publishOn(Scheduler)
    public void thread2() throws InterruptedException {
        Scheduler s = Schedulers.newParallel("myThreadPool",4);

        final Flux<Integer> flux = Flux
                .range(1, 10)
                .map(i ->
                    {
                        log.info("map 1 in " + Thread.currentThread().getName());
                        return i*2;
                    })
                .publishOn(s)
                .map(i ->
                    {
                        log.info("map 2 in " + Thread.currentThread().getName());
                        return i*2;
                    }
                )
                .publishOn(s)
                .map(i ->
                {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    log.info("map 3 in " + Thread.currentThread().getName());
                    return i/4;
                })
                .publishOn(s);

        new Thread(() ->
        {
            log.info("Subscribe in " + Thread.currentThread().getName());
            flux.subscribe((i) -> log.info("in subscribe value  -> " + i + ", thread -> " + Thread.currentThread().getName()));
        }).start();
        Thread.sleep(12000);
    }

    //subscribeOn(Scheduler) - run subscribe process in different thread. if u have publishOn - its change execution thread
    @Test
    public void thread3() throws InterruptedException {
        Scheduler s = Schedulers.newParallel("myThreadPool",4);

        final Flux<Integer> flux = Flux
                .range(1, 10)
                .map(i ->
                {
                    log.info("map 1 in " + Thread.currentThread().getName());
                    return i*2;
                })
                .publishOn(s)
                .subscribeOn(s)
                .map(i ->
                        {
                            log.info("map 2 in " + Thread.currentThread().getName());
                            return i*2;
                        }
                )
                .publishOn(s)
                .map(i ->
                {
                    log.info("map 3 in " + Thread.currentThread().getName());
                    return i/4;
                })
                .publishOn(s)
                .subscribeOn(s);

        new Thread(() ->
        {
            log.info("Subscribe in " + Thread.currentThread().getName());
            flux.subscribe((i) -> log.info("in subscribe value  -> " + i + ", thread -> " + Thread.currentThread().getName()));
        }).start();
        Thread.sleep(12000);
    }

}
