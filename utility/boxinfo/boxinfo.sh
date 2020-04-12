#!/bin/bash

thisdir=`(cd \`dirname $0\` > /dev/null 2>&1; pwd)`

#describe-capacity-reservations
#describe-hosts
#describe-instance-attribute
#describe-instances
#describe-volume-attribute
#describe-volumes

cmd=`which aws2`
if [ "${cmd}" == "" ]; then
	cmd=`which aws`
	if [ "${cmd}" == "" ]; then
		echo "ERROR: aws cli not installed"
		exit 255
	fi
fi	

instid=`ec2metadata --instance-id`
echo ===AWS describe-instances===
$cmd ec2 describe-instances --instance-ids ${instid}
echo ===AWS describe-volumes===
$cmd ec2 describe-volumes --filters Name=attachment.instance-id,Values=[${instid}]
echo ===sysinfo.sh===
${thisdir}/sysinfo.sh
