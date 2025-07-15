#!/usr/bin/env bash

#
# map-reduce tests
#

# un-comment this to run the tests with the Go race detector.
# RACE=-race

if [[ "$OSTYPE" = "darwin"* ]]
then
  if go version | grep 'go1.17.[012345]'
  then
    # -race with plug-ins on x86 MacOS 12 with
    # go1.17 before 1.17.6 sometimes crash.
    RACE=
    echo '*** Turning off -race since it may not work on a Mac'
    echo '    with ' `go version`
  fi
fi

ISQUIET=$1
maybe_quiet() {
    if [ "$ISQUIET" == "quiet" ]; then
      "$@" > /dev/null 2>&1
    else
      "$@"
    fi
}

TIMEOUT=timeout
TIMEOUT2=""
if timeout 2s sleep 1 > /dev/null 2>&1
then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1
  then
    TIMEOUT=gtimeout
  else
    # no timeout command
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]
then
  TIMEOUT2=$TIMEOUT
  TIMEOUT2+=" -k 2s 120s "
  TIMEOUT+=" -k 2s 45s "
fi

# run the test in a fresh sub-directory.
rm -rf output/temp
mkdir -p output/temp || exit 1
rm -f output/mr-*

# make sure software is freshly built.
(cd cmd && go clean)
(cd mrapps && go clean)
(cd mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd mrapps && go build $RACE -buildmode=plugin early_exit.go) || exit 1
(cd mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
(go build $RACE -o cmd/master cmd/master.go) || exit 1
(go build $RACE -o cmd/worker cmd/worker.go) || exit 1
(go build $RACE -o cmd/mrsequential cmd/mrsequential.go) || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output
./cmd/mrsequential mrapps/wc.so input/pg*.txt || exit 1
sort output/mr-out-0 > output/mr-correct-wc.txt
rm -f output/mr-out*

echo '***' Starting wc test.

maybe_quiet $TIMEOUT ./cmd/master mr -i "input/pg*txt" -p mrapps/wc.so -w 4 -r 1 -m 40000 &
pid=$!

# give the master time to start.
sleep 1

# start multiple workers.
(maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/wc.so -w 4 -r 1 -P 40001) &
(maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/wc.so -w 4 -r 1 -P 40002) &
(maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/wc.so -w 4 -r 1 -P 40003) &
(maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/wc.so -w 4 -r 1 -P 40004) &

# wait for the master to exit.
wait $pid

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort output/mr-out* | grep . > output/mr-wc-all
if cmp output/mr-wc-all output/mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and master to exit.
wait

#########################################################
# now indexer
rm -f output/mr-*

# generate the correct output
./cmd/mrsequential mrapps/indexer.so input/pg*.txt || exit 1
sort output/mr-out-0 > output/mr-correct-indexer.txt
rm -f output/mr-out*

echo '***' Starting indexer test.

maybe_quiet $TIMEOUT ./cmd/master mr -i "input/pg*txt" -p mrapps/indexer.so -w 4 -r 1 -m 40000 &
sleep 1

# start multiple workers
maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/indexer.so -w 4 -r 1 -P 40001 &
maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/indexer.so -w 4 -r 1 -P 40002 &
maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/indexer.so -w 4 -r 1 -P 40003 &
maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/indexer.so -w 4 -r 1 -P 40004

sort output/mr-out* | grep . > output/mr-indexer-all
if cmp output/mr-indexer-all output/mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting map parallelism test.

rm -f output/mr-*

maybe_quiet $TIMEOUT ./cmd/master mr -i "input/pg*txt" -p mrapps/mtiming.so -w 2 -r 1 -m 40000 &
sleep 1

maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/mtiming.so -w 2 -r 1 -P 40001 &
maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/mtiming.so -w 2 -r 1 -P 40002 &
wait
NT=`cat output/mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat output/mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting reduce parallelism test.

rm -f output/mr-*

maybe_quiet $TIMEOUT ./cmd/master mr -i "input/pg*txt" -p mrapps/rtiming.so -w 3 -r 3 -m 40000 &
sleep 1

maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/rtiming.so -w 3 -r 3 -P 40001 &
maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/rtiming.so -w 3 -r 3 -P 40002 &
maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/rtiming.so -w 3 -r 3 -P 40003 &
wait
NT=`cat output/mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait

#########################################################
echo '***' Starting job count test.

rm -f output/mr-*

maybe_quiet $TIMEOUT ./cmd/master mr -i "input/pg*txt" -p mrapps/jobcount.so -w 4 -r 1 -m 40000 &
sleep 1

maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/jobcount.so -w 4 -r 1 -P 40001 &
maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/jobcount.so -w 4 -r 1 -P 40002 &
maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/jobcount.so -w 4 -r 1 -P 40003 &
maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/jobcount.so -w 4 -r 1 -P 40004

wait
NT=`cat output/mr-out* | awk '{print $2}'`
if [ "$NT" -gt "1" ]
then
  echo '---' job count test: PASS
else
  echo '---' map jobs ran incorrect number of times "($NT < 2)"
  echo '---' job count test: FAIL
  failed_any=1
fi

rm -rf mr-worker-jobcount*
wait

#########################################################
# test whether any worker or master exits before the
# task has completed (i.e., all output files have been finalized)
rm -f output/mr-*

echo '***' Starting early exit test.

DF=anydone$$
rm -f $DF

(maybe_quiet $TIMEOUT ./cmd/master mr -i "input/pg*txt" -p mrapps/early_exit.so -w 4 -r 1 -m 40000; touch $DF) &

# give the master time to start.
sleep 1

# start multiple workers.
(maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/early_exit.so -w 4 -r 1 -P 40001; touch $DF) &
(maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/early_exit.so -w 4 -r 1 -P 40002; touch $DF) &
(maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/early_exit.so -w 4 -r 1 -P 40003; touch $DF) &
(maybe_quiet $TIMEOUT ./cmd/worker mr -i "input/pg*txt" -p mrapps/early_exit.so -w 4 -r 1 -P 40004; touch $DF) &

# wait for any of the master or workers to exit.
# `jobs` ensures that any completed old processes from other tests
# are not waited upon.
jobs &> /dev/null
if [[ "$OSTYPE" = "darwin"* ]]
then
  # bash on the Mac doesn't have wait -n
  while [ ! -e $DF ]
  do
    sleep 0.2
  done
else
  # the -n causes wait to wait for just one child process,
  # rather than waiting for all to finish.
  wait -n
fi

rm -f $DF

# a process has exited. this means that the output should be finalized
# otherwise, either a worker or the master exited early
sort output/mr-out* | grep . > output/mr-wc-all-initial

# wait for remaining workers and master to exit.
wait

# compare initial and final outputs
sort output/mr-out* | grep . > output/mr-wc-all-final
if cmp output/mr-wc-all-final output/mr-wc-all-initial
then
  echo '---' early exit test: PASS
else
  echo '---' output changed after first worker exited
  echo '---' early exit test: FAIL
  failed_any=1
fi
rm -f output/mr-*
rm -rf anydone*

#########################################################
echo '***' Starting crash test.

# generate the correct output
./cmd/mrsequential mrapps/nocrash.so input/pg*.txt || exit 1
sort output/mr-out-0 > output/mr-correct-crash.txt
rm -f output/mr-out*

rm -f output/mr-done

# Start master in background
(maybe_quiet $TIMEOUT2 ./cmd/master mr -i "input/pg*txt" -p mrapps/crash.so -w 1 -r 1 -m 40000; touch output/mr-done) &

sleep 1

# start multiple workers that may crash
maybe_quiet $TIMEOUT2 ./cmd/worker mr -i "input/pg*txt" -p mrapps/crash.so -w 1 -r 1 -P 40001 &

# Keep starting workers until job is done
port=40002
while [ ! -f output/mr-done ]
do
  maybe_quiet $TIMEOUT2 ./cmd/worker mr -i "input/pg*txt" -p mrapps/crash.so -w 1 -r 1 -P $port &
  port=$((port+1))
  if [ $port -gt 40025 ]; then
    port=40001
  fi
  sleep 1
done

wait

sort output/mr-out* | grep . > output/mr-crash-all
if cmp output/mr-crash-all output/mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

#########################################################
if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi