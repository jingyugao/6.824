#/bin/sh
for i in `seq 1 100`
do
    go test -run 2B >log/2b$i.log 2>&1 &
done
