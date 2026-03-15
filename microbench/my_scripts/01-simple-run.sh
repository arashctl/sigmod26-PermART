#! /bin/bash
binary=./bin/02-singly-linked-list.new.debra.pnone
nwork=16
nprefill=1
maxkey=2000000
prefillsize=500
millis=3000
$binary -nwork $nwork -nprefill $nprefill -i 25 -d 25 -rq 0 -rqsize 1 -k $maxkey -nrq 0 -t $millis -prefill-insert -prefillsize $prefillsize