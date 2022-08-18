import { O as Observable, i as isFunction, E as EmptyError, d as isScheduler, m as mapOneOrManyArgs, e as subscribeOn, o as observeOn, A as AsyncSubject, _ as __spreadArray, f as __read, h as defer, S as Subject, p as popResultSelector, j as innerFrom, k as createOperatorSubscriber, l as isArrayLike, n as mergeMap, q as scheduleIterable, t as __generator, u as identity, v as EMPTY, w as from, x as filter } from './index.js';
export { Z as ArgumentOutOfRangeError, A as AsyncSubject, B as BehaviorSubject, C as ConnectableObservable, v as EMPTY, E as EmptyError, ac as NEVER, $ as NotFoundError, T as Notification, U as NotificationKind, a0 as ObjectUnsubscribedError, O as Observable, R as ReplaySubject, N as Scheduler, a1 as SequenceError, S as Subject, Q as Subscriber, P as Subscription, a2 as TimeoutError, a3 as UnsubscriptionError, M as VirtualAction, V as VirtualTimeScheduler, K as animationFrame, L as animationFrameScheduler, z as animationFrames, D as asap, F as asapScheduler, G as async, H as asyncScheduler, a4 as concat, ae as concatAll, ad as config, af as debounceTime, h as defer, ag as distinctUntilChanged, ah as distinctUntilKeyChanged, a5 as empty, x as filter, Y as firstValueFrom, w as from, a6 as fromEventPattern, u as identity, ai as map, a7 as merge, aj as mergeAll, n as mergeMap, a8 as never, X as noop, y as observable, o as observeOn, a9 as of, W as pipe, ak as pluck, I as queue, J as queueScheduler, al as refCount, ab as scheduled, am as share, an as shareReplay, ao as skip, ap as startWith, e as subscribeOn, aq as switchMap, ar as take, as as takeUntil, aa as throwError, at as timeout, au as withLatestFrom } from './index.js';
import { a as argsArgArrayOrObject, c as createObject, o as onErrorResumeNext$1, b as argsOrArgArray, n as not } from './zipWith-109bf1ca.js';
export { e as audit, f as auditTime, g as buffer, h as bufferCount, j as bufferTime, k as bufferToggle, l as bufferWhen, m as catchError, p as combineAll, d as combineLatest, q as combineLatestAll, s as combineLatestWith, u as concatMap, v as concatMapTo, w as concatWith, x as connect, y as count, A as debounce, B as defaultIfEmpty, C as delay, D as delayWhen, E as dematerialize, F as distinct, G as elementAt, H as endWith, I as every, J as exhaust, K as exhaustAll, L as exhaustMap, M as expand, N as finalize, O as find, P as findIndex, Q as first, Y as flatMap, R as groupBy, S as ignoreElements, i as interval, T as isEmpty, U as last, V as mapTo, W as materialize, X as max, Z as mergeMapTo, _ as mergeScan, $ as mergeWith, a0 as min, a1 as multicast, a2 as pairwise, a3 as publish, a4 as publishBehavior, a5 as publishLast, a6 as publishReplay, r as race, a7 as raceWith, a8 as reduce, a9 as repeat, aa as repeatWhen, ab as retry, ac as retryWhen, ad as sample, ae as sampleTime, af as scan, ag as sequenceEqual, ah as single, ai as skipLast, aj as skipUntil, ak as skipWhile, al as switchAll, am as switchMapTo, an as switchScan, ao as takeLast, ap as takeWhile, aq as tap, ar as throttle, as as throttleTime, at as throwIfEmpty, au as timeInterval, av as timeoutWith, t as timer, aw as timestamp, ax as toArray, ay as window, az as windowCount, aA as windowTime, aB as windowToggle, aC as windowWhen, z as zip, aD as zipAll, aE as zipWith } from './zipWith-109bf1ca.js';

function isObservable(obj) {
    return !!obj && (obj instanceof Observable || (isFunction(obj.lift) && isFunction(obj.subscribe)));
}

function lastValueFrom(source, config) {
    var hasConfig = typeof config === 'object';
    return new Promise(function (resolve, reject) {
        var _hasValue = false;
        var _value;
        source.subscribe({
            next: function (value) {
                _value = value;
                _hasValue = true;
            },
            error: reject,
            complete: function () {
                if (_hasValue) {
                    resolve(_value);
                }
                else if (hasConfig) {
                    resolve(config.defaultValue);
                }
                else {
                    reject(new EmptyError());
                }
            },
        });
    });
}

function bindCallbackInternals(isNodeStyle, callbackFunc, resultSelector, scheduler) {
    if (resultSelector) {
        if (isScheduler(resultSelector)) {
            scheduler = resultSelector;
        }
        else {
            return function () {
                var args = [];
                for (var _i = 0; _i < arguments.length; _i++) {
                    args[_i] = arguments[_i];
                }
                return bindCallbackInternals(isNodeStyle, callbackFunc, scheduler)
                    .apply(this, args)
                    .pipe(mapOneOrManyArgs(resultSelector));
            };
        }
    }
    if (scheduler) {
        return function () {
            var args = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                args[_i] = arguments[_i];
            }
            return bindCallbackInternals(isNodeStyle, callbackFunc)
                .apply(this, args)
                .pipe(subscribeOn(scheduler), observeOn(scheduler));
        };
    }
    return function () {
        var _this = this;
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        var subject = new AsyncSubject();
        var uninitialized = true;
        return new Observable(function (subscriber) {
            var subs = subject.subscribe(subscriber);
            if (uninitialized) {
                uninitialized = false;
                var isAsync_1 = false;
                var isComplete_1 = false;
                callbackFunc.apply(_this, __spreadArray(__spreadArray([], __read(args)), [
                    function () {
                        var results = [];
                        for (var _i = 0; _i < arguments.length; _i++) {
                            results[_i] = arguments[_i];
                        }
                        if (isNodeStyle) {
                            var err = results.shift();
                            if (err != null) {
                                subject.error(err);
                                return;
                            }
                        }
                        subject.next(1 < results.length ? results : results[0]);
                        isComplete_1 = true;
                        if (isAsync_1) {
                            subject.complete();
                        }
                    },
                ]));
                if (isComplete_1) {
                    subject.complete();
                }
                isAsync_1 = true;
            }
            return subs;
        });
    };
}

function bindCallback(callbackFunc, resultSelector, scheduler) {
    return bindCallbackInternals(false, callbackFunc, resultSelector, scheduler);
}

function bindNodeCallback(callbackFunc, resultSelector, scheduler) {
    return bindCallbackInternals(true, callbackFunc, resultSelector, scheduler);
}

var DEFAULT_CONFIG = {
    connector: function () { return new Subject(); },
    resetOnDisconnect: true,
};
function connectable(source, config) {
    if (config === void 0) { config = DEFAULT_CONFIG; }
    var connection = null;
    var connector = config.connector, _a = config.resetOnDisconnect, resetOnDisconnect = _a === void 0 ? true : _a;
    var subject = connector();
    var result = new Observable(function (subscriber) {
        return subject.subscribe(subscriber);
    });
    result.connect = function () {
        if (!connection || connection.closed) {
            connection = defer(function () { return source; }).subscribe(subject);
            if (resetOnDisconnect) {
                connection.add(function () { return (subject = connector()); });
            }
        }
        return connection;
    };
    return result;
}

function forkJoin() {
    var args = [];
    for (var _i = 0; _i < arguments.length; _i++) {
        args[_i] = arguments[_i];
    }
    var resultSelector = popResultSelector(args);
    var _a = argsArgArrayOrObject(args), sources = _a.args, keys = _a.keys;
    var result = new Observable(function (subscriber) {
        var length = sources.length;
        if (!length) {
            subscriber.complete();
            return;
        }
        var values = new Array(length);
        var remainingCompletions = length;
        var remainingEmissions = length;
        var _loop_1 = function (sourceIndex) {
            var hasValue = false;
            innerFrom(sources[sourceIndex]).subscribe(createOperatorSubscriber(subscriber, function (value) {
                if (!hasValue) {
                    hasValue = true;
                    remainingEmissions--;
                }
                values[sourceIndex] = value;
            }, function () { return remainingCompletions--; }, undefined, function () {
                if (!remainingCompletions || !hasValue) {
                    if (!remainingEmissions) {
                        subscriber.next(keys ? createObject(keys, values) : values);
                    }
                    subscriber.complete();
                }
            }));
        };
        for (var sourceIndex = 0; sourceIndex < length; sourceIndex++) {
            _loop_1(sourceIndex);
        }
    });
    return resultSelector ? result.pipe(mapOneOrManyArgs(resultSelector)) : result;
}

var nodeEventEmitterMethods = ['addListener', 'removeListener'];
var eventTargetMethods = ['addEventListener', 'removeEventListener'];
var jqueryMethods = ['on', 'off'];
function fromEvent(target, eventName, options, resultSelector) {
    if (isFunction(options)) {
        resultSelector = options;
        options = undefined;
    }
    if (resultSelector) {
        return fromEvent(target, eventName, options).pipe(mapOneOrManyArgs(resultSelector));
    }
    var _a = __read(isEventTarget(target)
        ? eventTargetMethods.map(function (methodName) { return function (handler) { return target[methodName](eventName, handler, options); }; })
        :
            isNodeStyleEventEmitter(target)
                ? nodeEventEmitterMethods.map(toCommonHandlerRegistry(target, eventName))
                : isJQueryStyleEventEmitter(target)
                    ? jqueryMethods.map(toCommonHandlerRegistry(target, eventName))
                    : [], 2), add = _a[0], remove = _a[1];
    if (!add) {
        if (isArrayLike(target)) {
            return mergeMap(function (subTarget) { return fromEvent(subTarget, eventName, options); })(innerFrom(target));
        }
    }
    if (!add) {
        throw new TypeError('Invalid event target');
    }
    return new Observable(function (subscriber) {
        var handler = function () {
            var args = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                args[_i] = arguments[_i];
            }
            return subscriber.next(1 < args.length ? args : args[0]);
        };
        add(handler);
        return function () { return remove(handler); };
    });
}
function toCommonHandlerRegistry(target, eventName) {
    return function (methodName) { return function (handler) { return target[methodName](eventName, handler); }; };
}
function isNodeStyleEventEmitter(target) {
    return isFunction(target.addListener) && isFunction(target.removeListener);
}
function isJQueryStyleEventEmitter(target) {
    return isFunction(target.on) && isFunction(target.off);
}
function isEventTarget(target) {
    return isFunction(target.addEventListener) && isFunction(target.removeEventListener);
}

function generate(initialStateOrOptions, condition, iterate, resultSelectorOrScheduler, scheduler) {
    var _a, _b;
    var resultSelector;
    var initialState;
    if (arguments.length === 1) {
        (_a = initialStateOrOptions, initialState = _a.initialState, condition = _a.condition, iterate = _a.iterate, _b = _a.resultSelector, resultSelector = _b === void 0 ? identity : _b, scheduler = _a.scheduler);
    }
    else {
        initialState = initialStateOrOptions;
        if (!resultSelectorOrScheduler || isScheduler(resultSelectorOrScheduler)) {
            resultSelector = identity;
            scheduler = resultSelectorOrScheduler;
        }
        else {
            resultSelector = resultSelectorOrScheduler;
        }
    }
    function gen() {
        var state;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    state = initialState;
                    _a.label = 1;
                case 1:
                    if (!(!condition || condition(state))) return [3, 4];
                    return [4, resultSelector(state)];
                case 2:
                    _a.sent();
                    _a.label = 3;
                case 3:
                    state = iterate(state);
                    return [3, 1];
                case 4: return [2];
            }
        });
    }
    return defer((scheduler
        ?
            function () { return scheduleIterable(gen(), scheduler); }
        :
            gen));
}

function iif(condition, trueResult, falseResult) {
    return defer(function () { return (condition() ? trueResult : falseResult); });
}

function onErrorResumeNext() {
    var sources = [];
    for (var _i = 0; _i < arguments.length; _i++) {
        sources[_i] = arguments[_i];
    }
    return onErrorResumeNext$1(argsOrArgArray(sources))(EMPTY);
}

function pairs(obj, scheduler) {
    return from(Object.entries(obj), scheduler);
}

function partition(source, predicate, thisArg) {
    return [filter(predicate, thisArg)(innerFrom(source)), filter(not(predicate, thisArg))(innerFrom(source))];
}

function range(start, count, scheduler) {
    if (count == null) {
        count = start;
        start = 0;
    }
    if (count <= 0) {
        return EMPTY;
    }
    var end = count + start;
    return new Observable(scheduler
        ?
            function (subscriber) {
                var n = start;
                return scheduler.schedule(function () {
                    if (n < end) {
                        subscriber.next(n++);
                        this.schedule();
                    }
                    else {
                        subscriber.complete();
                    }
                });
            }
        :
            function (subscriber) {
                var n = start;
                while (n < end && !subscriber.closed) {
                    subscriber.next(n++);
                }
                subscriber.complete();
            });
}

function using(resourceFactory, observableFactory) {
    return new Observable(function (subscriber) {
        var resource = resourceFactory();
        var result = observableFactory(resource);
        var source = result ? innerFrom(result) : EMPTY;
        source.subscribe(subscriber);
        return function () {
            if (resource) {
                resource.unsubscribe();
            }
        };
    });
}

export { bindCallback, bindNodeCallback, connectable, forkJoin, fromEvent, generate, iif, isObservable, lastValueFrom, onErrorResumeNext, pairs, partition, range, using };
//# sourceMappingURL=index-98c41060.js.map
