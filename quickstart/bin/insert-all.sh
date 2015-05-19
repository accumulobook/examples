#!/bin/bash


run_mvn() {
  local instanceName=$1
  local zkLocation=$2
  local tableName=$3

  mvn clean compile exec:exec -Drow.id="bob jones" \
  -Dcolumn.family=contact \
  -Dcolumn.qualifier=address \
  -Dauths=billing \
  -Dvalue="123 any street" \
  -Dtable.name="${tableName}" \
  -Dinstance.name="${instanceName}" -Dzookeeper.location="${zkLocation}" -Pjava:insert

  mvn clean compile exec:exec -Drow.id="bob jones" \
  -Dcolumn.family=contact \
  -Dcolumn.qualifier=city \
  -Dauths=billing \
  -Dvalue="anytown" \
  -Dtable.name="${tableName}" \
  -Dinstance.name="${instanceName}" -Dzookeeper.location="${zkLocation}" -Pjava:insert

  mvn clean compile exec:exec -Drow.id="bob jones" \
  -Dcolumn.family=contact \
  -Dcolumn.qualifier=phone \
  -Dauths=billing \
  -Dvalue="555-1212" \
  -Dtable.name="${tableName}" \
  -Dinstance.name="${instanceName}" -Dzookeeper.location="${zkLocation}" -Pjava:insert

  mvn clean compile exec:exec -Drow.id="bob jones" \
  -Dcolumn.family=purchases \
  -Dcolumn.qualifier=sneakers \
  -Dauths=billing\&inventory \
  -Dvalue="\$60" \
  -Dtable.name="${tableName}" \
  -Dinstance.name="${instanceName}" -Dzookeeper.location="${zkLocation}" -Pjava:insert

  mvn clean compile exec:exec -Drow.id="fred smith" \
  -Dcolumn.family=contact \
  -Dcolumn.qualifier=address \
  -Dauths=billing \
  -Dvalue="444 main st." \
  -Dtable.name="${tableName}" \
  -Dinstance.name="${instanceName}" -Dzookeeper.location="${zkLocation}" -Pjava:insert

  mvn clean compile exec:exec -Drow.id="fred smith" \
  -Dcolumn.family=contact \
  -Dcolumn.qualifier=city \
  -Dauths=billing \
  -Dvalue="othertown" \
  -Dtable.name="${tableName}" \
  -Dinstance.name="${instanceName}" -Dzookeeper.location="${zkLocation}" -Pjava:insert

  mvn clean compile exec:exec -Drow.id="fred smith" \
  -Dcolumn.family=purchases \
  -Dcolumn.qualifier=glasses \
  -Dauths=billing\&inventory \
  -Dvalue="\$30" \
  -Dtable.name="${tableName}" \
  -Dinstance.name="${instanceName}" -Dzookeeper.location="${zkLocation}" -Pjava:insert

  mvn clean compile exec:exec -Drow.id="fred smith" \
  -Dcolumn.family=purchases \
  -Dcolumn.qualifier=hat \
  -Dauths=billing\&inventory \
  -Dvalue="\$20" \
  -Dtable.name="${tableName}" \
  -Dinstance.name="${instanceName}" -Dzookeeper.location="${zkLocation}" -Pjava:insert
}

if [[ "$#" -ne 3 ]]; then
  echo "You need 3 argument, the first it the instanceName and the second is the zookeeper location and the third is the table name"
  echo "Try running with something like this:"
  echo ""
  echo "    $0 miniInstance localhost:12345 table2"
  exit 1
else
  run_mvn "${1}" "${2}" "${3}"
fi
