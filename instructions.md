Building Airflow ETL Pipeline

Create ETL pipeline using ASTRO CLI, in which your task is to integrate daily currency exchange
rates to mongodb after transformations.
API details are:
Use Frankfurter API, a free public currency exchange-rate API.
Website: https://frankfurter.dev/
API: https://api.frankfurter.dev
One API call is demonstrated as:
https://api.frankfurter.dev/v2/rates?base=EUR&amp;quotes=USD,GBP,JPY

MongoDB and Mongo Express Configuration
For this assignment, you must use MongoDB as the primary database and Mongo Express as its
web-based administration interface.
Configure Mongo Express as another Docker service.

Add following meta details to your docker images
a) Docker Networking
b) Persistent Storage
Create Three (3) tasks
a) Extract Currency Exchange Rates
b) Transform Exchange Rate Data
Transformed data should contain following details:
{
&quot;base_currency&quot;: &quot;EUR&quot;,
&quot;target_currency&quot;: &quot;USD&quot;,
&quot;exchange_rate&quot;: 1.15,
&quot;rate_date&quot;: &quot;2026-11-04&quot;,
&quot;source&quot;: &quot;Frankfurter API&quot;,
&quot;retrieved_at&quot;: &quot;2026-11-05T07:15:00&quot;

}
c) Load Data into MongoDB
You must use Airflow (Named) XCom to transfer data between tasks.
Submission Instructions
a) Create a report containing screenshots of code and outputs.
b) Push the code to GITHUB

keep database local only.