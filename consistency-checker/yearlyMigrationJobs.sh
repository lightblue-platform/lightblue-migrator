#!/usr/bin/env bash
set -e

ENTITY_NAME=$1
START_DATE=$2
END_DATE=$3
NUM_HOURS=$4
EXPECTED_EXECUTION_TIME=$5
CREATED_BY=$6
STARTING_ID=$7
INCREMENT_ID=$8

if [ $1"x" == "x" ] || [ $2"x" == "x" ] || [ $3"x" == "x" ] || [ $4"x" == "x" ] || [ $5"x" == "x" ] || [ $6"x" == "x" ]; then
    echo "Usage: ./yearlyMigrationJobs.sh <entityName> <startDate> <endDate> <frequency> <expectedExecutionTime> <createdBy> [<startingId>] [<yearlyIdIncrement>]"
    echo "Example: ./yearlyMigrationJobs.sh user 2014-01-01 2014-12-31 1 30000 derek63 0 5000 "
    echo "Results will be stored in /tmp/jobs/"
    exit 1
fi

echo "Processing (results will be available on /tmp/jobs/)"

if [ "${STARTING_ID}x" == "x" ]; then
    STARTING_ID=0
fi

if [ "${INCREMENT_ID}x" == "x" ]; then
    INCREMENT_ID=10000
fi

STRING_DATE_FORMAT="+%Y-%m-%d"

mkdir -p /tmp/jobs

startDate=$( date $STRING_DATE_FORMAT -d "$START_DATE" )
remainingDays=$((10#$(date -d "Dec 31 `date +%Y -d $START_DATE `" +%j) - 10#$(date +%j -d $START_DATE ) ))
endDate=$( date $STRING_DATE_FORMAT -d "$startDate +$remainingDays days" )
runOnce=0
if [ "$endDate" \> "$END_DATE" ]; then
    endDate=$END_DATE
    runOnce=1
fi
currentYear=$(date +%Y -d $START_DATE)
nextYear=$((currentYear+1))

i=$STARTING_ID
while true; do
    sh migrationJobs.sh $ENTITY_NAME $startDate $endDate $NUM_HOURS $EXPECTED_EXECUTION_TIME $CREATED_BY $i > /tmp/jobs/termsAcknowledgementJobs${currentYear}.json
    startDate=$( date $STRING_DATE_FORMAT -d "$endDate +1 day" )
    endDate=$nextYear"-12-31"
    if [ $runOnce -eq 1 ];then
        break
    fi
    if [ "$endDate"  \> "$END_DATE" ];then
        endDate=$END_DATE
        runOnce=1
    elif [ "$endDate" == "$END_DATE" ];then
        runOnce=1
    fi
    i=$((i+INCREMENT_ID))
    currentYear=$nextYear
    nextYear=$((nextYear+1))
done
