# Official List

### Tool that will maintain a list of all US Senators and Representatives

<p> Its a k8s cron job that checks existing members of the house and senate daily and comapares that to yeserdays list.
With some configuration, it will write to a stateful postgres instance in kubernetes and then check for a diff between daily calls and previous records</p>


### This is a WIP