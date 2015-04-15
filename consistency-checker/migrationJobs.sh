#!/usr/bin/env bash
set -e

ENTITY_NAME=$1
START_DATE=$2
END_DATE=$3
NUM_HOURS=$4
EXPECTED_EXECUTION_TIME=$5
CREATED_BY=$6
STARTING_ID=$7

if [ $1"x" == "x" ] || [ $2"x" == "x" ] || [ $3"x" == "x" ] || [ $4"x" == "x" ] || [ $5"x" == "x" ] || [ $6"x" == "x" ]; then
    echo "Usage: ./migrationJobs.sh <entityName> <startDate> <endDate> <frequency> <expectedExecutionTime> <createdBy> [<startingId>]"
    echo "Example: ./migrationJobs.sh user 2014-01-01 2014-12-31 1 30000 derek63 0"
    exit 1
fi

if [ "${STARTING_ID}x" == "x" ]; then
    STARTING_ID=0
fi

STRING_DATE_FORMAT="+%Y-%m-%dT%H:%M:%S%z"
OUTPUT_DATE_FORMAT="+%Y%m%dT%H:%M:%S.000%z"

echo {
echo  \"data\": [
i=$STARTING_ID
now=$(date $OUTPUT_DATE_FORMAT)
startDate=$( date $STRING_DATE_FORMAT --date "$START_DATE" )
while true; do
    endDate=$( date $STRING_DATE_FORMAT --date "$startDate +$NUM_HOURS hours" );
    whenAvailableDate=$( date $STRING_DATE_FORMAT --date "$endDate +1 minutes" );
    echo {
    echo  \"_id\": \""$ENTITY_NAME"Job_"$i"\",
    echo  \"objectType\": \"migrationJob\",
    echo  \"configurationName\": \""$ENTITY_NAME"\",
    echo  \"startDate\": \"$(date $OUTPUT_DATE_FORMAT --date "$startDate")\",
    echo  \"endDate\": \"$(date $OUTPUT_DATE_FORMAT --date "$endDate")\",
    echo  \"whenAvailableDate\": \"$(date $OUTPUT_DATE_FORMAT --date "$whenAvailableDate")\",
    echo  \"expectedExecutionMilliseconds\" : $EXPECTED_EXECUTION_TIME,
    echo  \"jobExecutions\": [],
    echo  \"creationDate\": \"$now\",
    echo  \"createdBy\": \"$CREATED_BY\",
    echo  \"lastUpdateDate\": \"$now\",
    echo  \"lastUpdatedBy\": \"$CREATED_BY\"
    echo }
    startDate="$endDate"
    [ "$endDate" \< "$END_DATE" ] || break
    i=$((i+1))
    echo ,
done

echo  ],
echo  \"projection\": [
echo    {
echo      \"field\": \"_id\",
echo      \"include\": true,
echo      \"recursive\": false
echo    }
echo  ]
echo  }
