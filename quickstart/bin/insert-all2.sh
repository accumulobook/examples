
#!/bin/bash

# script to run the JavaExample insert command and insert some sample data

# current directory
_script_dir() {
    if [ -z "${SCRIPT_DIR}" ]; then
    # even resolves symlinks, see
    # http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
        local SOURCE="${BASH_SOURCE[0]}"
        while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done
        SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    fi
    echo "${SCRIPT_DIR}"
}

INSTANCE=accumulo
ZK_LOC=localhost:2181
USER=root
PASS=secret
CMD="accumulo -add $(_script_dir)/../target/mini-accumulo-cluster-example-0.0.1-SNAPSHOT.jar com.accumulobook.macexample.JavaExample -i ${INSTANCE} -z ${ZK_LOC} -u ${USER} -p ${PASS}"

${CMD} insert -r "bob jones" -t table3 -cq contact -cf address -val "123 any street" -a billing
${CMD} insert -r "bob jones" -t table3 -cq contact -cf city -val anytown -a billing
${CMD} insert -r "bob jones" -t table3 -cq contact -cf phone -val "555-1212" -a billing
${CMD} insert -r "bob jones" -t table3 -cq purchases -cf sneakers -val "\$60" -a "billing&inventory"
${CMD} insert -r "fred smith" -t table3 -cq contact -cf address -val "444 main st." -a billing
${CMD} insert -r "fred smith" -t table3 -cq contact -cf city -val "othertown" -a billing
${CMD} insert -r "fred smith" -t table3 -cq purchases -cf glasses -val "\$30" -a  "billing&inventory"
${CMD} insert -r "fred smith" -t table3 -cq purchases -cf hat -val "\$20" -a "billing&inventory"
