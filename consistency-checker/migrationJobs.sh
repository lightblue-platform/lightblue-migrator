ENTITY_NAME=$1
START_DATE=$2
END_DATE=$3
NUM_HOURS=$4
EXPECTED_EXECUTION_TIME=$5
CREATED_BY=$6
AVAILABLE_DATE=$7

if [ $1"x" == "x" ] || [ $2"x" == "x" ] || [ $3"x" == "x" ] || [ $4"x" == "x" ] || [ $5"x" == "x" ] || [ $6"x" == "x" ]; then
    echo "Usage: ./migrationJobs.sh <entityName> <startDate> <endDate> <frequency> <expectedExecutionTime> <createdBy> <availableDate>"
    echo "Example: ./migrationJobs.sh user 2014-01-01 2014-12-31 1 30000 derek63 2015-03-01"
    exit 1
fi
echo {
echo  \"data\": [
i=0
now=$(date)
current="$START_DATE"
while true; do
    next=$( date +%Y-%m-%dT%H:%M:%S%z --date "$current +$NUM_HOURS hours" );
    [ "$next" \< "$END_DATE" ] || next="$END_DATE"
    echo {
    echo  \"_id\": \""$ENTITY_NAME"Job_"$i"\",
    echo  \"objectType\": \"migrationJob\",
    echo  \"configurationName\": \""$ENTITY_NAME"\",
    echo  \"startDate\": \"$( date +%Y%m%dT%H:%M:%S.000%z --date "$current")\",
    echo  \"endDate\": \"$( date +%Y%m%dT%H:%M:%S.000%z --date "$next")\",
    [ $AVAILABLE_DATE"x" == "x" ] || echo  \"whenAvailableDate\" : \"$( date +%Y%m%dT%H:%M:%S.000%z --date "$AVAILABLE_DATE")\",
    [ $AVAILABLE_DATE"x" != "x" ] || echo  \"whenAvailableDate\" : \"$( date +%Y%m%dT%H:%M:%S.000%z --date "$next")\",
    echo  \"expectedExecutionMilliseconds\" : $EXPECTED_EXECUTION_TIME,
    echo  \"jobExecutions\": [],
    echo  \"creationDate\": \"$( date +%Y%m%dT%H:%M:%S.000%z --date "$now")\",
    echo  \"createdBy\": \"$CREATED_BY\",
    echo  \"lastUpdateDate\": \"$( date +%Y%m%dT%H:%M:%S.000%z --date "$now")\",
    echo  \"lastUpdatedBy\": \"$CREATED_BY\"
    echo }
    current="$next"
    [ "$next" \< "$END_DATE" ] || break
    let i=i+1
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
