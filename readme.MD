# Official List

### Tool that will maintain a list of all US Senators and Representatives

<p> Its a k8s 'microservice' that checks existing members of the house and senate daily and compares that to yesterdays list.
It writes to a stateful postgres instance in kubernetes and then checks for a diff between its daily calls to the existing members endpoint and previously stored records in the statefulset. If a diff exists it will update the statefulset by adding the new members</p>


#### Background
<p>I need to maintain a list of these members so I can check for recent stock trades made by legislators for copy trading. Basically I iterate through this list from another microservice that scrapes congresional stock trading disclosure data by congressional member, and writes to a db and then looks for diffs between previously known disclousers and new data. If new data exists, that indicates a trade has been placed and a payload will be sent to the notification service.

 The notification service lets me know when a new disclosure is detected so I can take action. Id like to build out a suite of services that scan legislation for keywords relating to ticker symbols to try and predict what legislation will pass based on congressional stock trade activity. If you want to contribute please let me know!</p>