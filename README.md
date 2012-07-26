Timed supervisor
================

http://forum.trapexit.org/viewtopic.php?p=44147#44147


This is a special supervisor for Erlang (http://erlang.org/) that allows you to run child by schedule.
For example, you may want to capture stock quotes from security exchange only from 8:00 UTC till 16:00 UTC.
This code will keep your child running only at required time.


timed_supervisor.erl is intended for tracking for only one child at a time. It is impossible to add several
children to it. Launch several timed_supervisors if you need several children.



Initial code is written by Serge Aleynikov <saleyn@gmail.com>
This code is with fixes for using UTC instead of local time, written by Danila Zagoskin <z@gosk.in>.
