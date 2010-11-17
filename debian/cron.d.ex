#
# Regular cron jobs for the arakoon package
#
0 4	* * *	root	[ -x /usr/bin/arakoon_maintenance ] && /usr/bin/arakoon_maintenance
