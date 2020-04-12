#!/bin/bash

# Copyright (c) Oktay Technology 2013 All rights reserved.

is_ubuntu=`cat /etc/issue | grep -ci Ubuntu`

{
        while read cmd; do
                if expr "$cmd" : '^redhat:' > /dev/null; then
                        if [ $is_ubuntu -eq 1 ]; then
                                continue
                        fi
                        cmd=`echo "$cmd" | sed 's/^redhat:[ ]*//'`
                elif expr "$cmd" : '^ubuntu:' > /dev/null; then
                        if [ $is_ubuntu -eq 0 ]; then
                                continue
                        fi
                        cmd=`echo "$cmd" | sed 's/^ubuntu:[ ]*//'`
                fi
                echo "=== $cmd ==="
                eval $cmd
        done
} <<- EOD
cat /etc/issue
redhat: cat /etc/redhat-release
cat /proc/version
uname -a
cat /proc/cmdline
redhat: rpm -qa | grep gcc
ubuntu: aptitude search '~i!~M' | grep gcc
redhat: rpm -qa | grep glibc
ubuntu: aptitude search '~i!~M' | grep glibc
sudo sysctl -a
lscpu
cat /proc/cpuinfo
cat /proc/meminfo
lspci -v
lspci -tv
free
df -h
mount -l
fdisk -l
cat /etc/fstab
dmidecode | grep -v "Serial Number"
EOD
