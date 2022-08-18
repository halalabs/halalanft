import { x as filter, _ as __spreadArray, f as __read } from './index.js';
export { ae as concatAll, af as debounceTime, ag as distinctUntilChanged, ah as distinctUntilKeyChanged, x as filter, ai as map, aj as mergeAll, n as mergeMap, o as observeOn, ak as pluck, al as refCount, am as share, an as shareReplay, ao as skip, ap as startWith, e as subscribeOn, aq as switchMap, ar as take, as as takeUntil, at as timeout, au as withLatestFrom } from './index.js';
import { n as not, a7 as raceWith, b as argsOrArgArray } from './zipWith-109bf1ca.js';
export { e as audit, f as auditTime, g as buffer, h as bufferCount, j as bufferTime, k as bufferToggle, l as bufferWhen, m as catchError, p as combineAll, aF as combineLatest, q as combineLatestAll, s as combineLatestWith, aG as concat, u as concatMap, v as concatMapTo, w as concatWith, x as connect, y as count, A as debounce, B as defaultIfEmpty, C as delay, D as delayWhen, E as dematerialize, F as distinct, G as elementAt, H as endWith, I as every, J as exhaust, K as exhaustAll, L as exhaustMap, M as expand, N as finalize, O as find, P as findIndex, Q as first, Y as flatMap, R as groupBy, S as ignoreElements, T as isEmpty, U as last, V as mapTo, W as materialize, X as max, aH as merge, Z as mergeMapTo, _ as mergeScan, $ as mergeWith, a0 as min, a1 as multicast, o as onErrorResumeNext, a2 as pairwise, a3 as publish, a4 as publishBehavior, a5 as publishLast, a6 as publishReplay, a7 as raceWith, a8 as reduce, a9 as repeat, aa as repeatWhen, ab as retry, ac as retryWhen, ad as sample, ae as sampleTime, af as scan, ag as sequenceEqual, ah as single, ai as skipLast, aj as skipUntil, ak as skipWhile, al as switchAll, am as switchMapTo, an as switchScan, ao as takeLast, ap as takeWhile, aq as tap, ar as throttle, as as throttleTime, at as throwIfEmpty, au as timeInterval, av as timeoutWith, aw as timestamp, ax as toArray, ay as window, az as windowCount, aA as windowTime, aB as windowToggle, aC as windowWhen, aI as zip, aD as zipAll, aE as zipWith } from './zipWith-109bf1ca.js';

function partition(predicate, thisArg) {
    return function (source) {
        return [filter(predicate, thisArg)(source), filter(not(predicate, thisArg))(source)];
    };
}

function race() {
    var args = [];
    for (var _i = 0; _i < arguments.length; _i++) {
        args[_i] = arguments[_i];
    }
    return raceWith.apply(void 0, __spreadArray([], __read(argsOrArgArray(args))));
}

export { partition, race };
//# sourceMappingURL=index-f83064b7.js.map
