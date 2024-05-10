![image](https://github.com/TranTienProVip123/Connect-Spark-to-a-database-perform-analytics-place-review-from-google-map-in-a-Flask-app/assets/169421431/66e7f38e-1f94-4010-a329-a11c41f65050)# Connect-Spark-to-a-database-perform-analytics-place-review-from-google-map-in-a-Flask-app
Build demo using the Flask-app to visualize analyzed reviews, use Spark to analyze and make evaluation predictions.

INTRODUCE
In this project, we focus on connecting Apache Spark to the database, performing data analysis, and displaying review prices from Google Map in a Flask application.
1. Project goals:
Connecting Apache Spark to the database: We will establish a connection to the database to extract the data needed for analysis.
Perform data analysis: Using Apache Spark, we will perform complex analyzes on the database, aiming to better understand the information stored and generate meaningful conclusions.
Displaying review prices from Google Map in a Flask application: We will integrate the review values ​​from Google Map into a website application using Flask, making it easy for users to access and interact with this information .
2. Reasons for choosing materials:
We chose this topic because it combines a variety of technologies and skills, from working with databases to data analysis using Apache Spark to web application development with Flask. Besides, the unified review tool from Google Map also brings added value to our application, helping users have an overview and details about the places or products they are interested in. heart. At the same time, this project is also an opportunity for us to apply our knowledge of big data and develop web programming skills.

CHAPTER I: SPARK THEORETICAL BASIS
1.1. What is Apache Spark?
Apache Spark is an open source distributed processing system used for big data workloads. The system uses in-memory write caching and optimized query execution to help queries quickly parse data of any size. Apache Spark provides development APIs in Java, Scala, Python, and R and supports code reuse across a variety of workloads, such as batch processing, interactive queries, and real-time analytics. real-time, machine learning and graph processing. You'll find this system used by organizations in every industry, including at FINRA, Yelp, Zillow, DataXu, Urban Institute, and CrowdStrike. [first]

CHAPTER II: CONNECT DATABASE WITH SPARK TO ANALYZE AND GIVE PREDICTED RESULTS BASED ON GOOGLEMAP REVIEWS
2.1. Statement of the Problem
- Our team used MongoDB to store data and connect with spark to use spark to analyze large amounts of data of google map review.
- Provide a visual view on the Flask-App interface, use spark to analyze the number of customer review stars on google map and make predictions about review trends, giving an overall view to customers.
2.2. Do the maths:
- The problem of connecting the MongoDB database with Apache Spark to analyze and make predictions based on reviews on Google Maps.
- To connect the MongoDB database with Apache Spark, we use the Spark Connector for MongoDB package. This is a tool package that allows reading and writing data from MongoDB through Spark.
- After installing Spark Connector for MongoDB package, we use its API to read data from MongoDB into Spark DataFrame. Spark DataFrame is a powerful distributed data structure in Spark that allows performing data transformations and analytics.
- After reading data from MongoDB into Spark DataFrame, use Spark features to analyze the data.
-Install the necessary libraries by pip install the requirements.txt file.
- For making predictions based on reviews on Google Maps, machine learning algorithms available in Spark such as Spark MLlib or Spark ML can be used to train the prediction model.
- Post Flask-App interface, visualize graphs of reviews on google map to give readers an overview.
- After training the prediction model, use that model to make predictions on new data. Prediction results can be stored in MongoDB or returned to our application for display or other use

DEMO:
1.Use mongodb to download data from google map to push it through apache spark for processing
![image](https://github.com/TranTienProVip123/Connect-Spark-to-a-database-perform-analytics-place-review-from-google-map-in-a-Flask-app/assets/169421431/5e6344af-0596-4f3d-9221-08ecbb9a4450)
2.result
![image](https://github.com/TranTienProVip123/Connect-Spark-to-a-database-perform-analytics-place-review-from-google-map-in-a-Flask-app/assets/169421431/d0c41d8a-32fe-4c96-9a2e-7413eac65cdc)
![image](https://github.com/TranTienProVip123/Connect-Spark-to-a-database-perform-analytics-place-review-from-google-map-in-a-Flask-app/assets/169421431/443a710e-7150-4964-8478-a98e68462c73)
3.connect , cluster apache spark
![image](https://github.com/TranTienProVip123/Connect-Spark-to-a-database-perform-analytics-place-review-from-google-map-in-a-Flask-app/assets/169421431/e01008e6-231c-4fa5-814c-ae9700c57cfa)

*Set core , gb  ,ram worker
![image](https://github.com/TranTienProVip123/Connect-Spark-to-a-database-perform-analytics-place-review-from-google-map-in-a-Flask-app/assets/169421431/9b138a9e-1b0d-44ba-850e-7802e8c0538c)

-Output to the screen includes 4 workers, including total core, memory, and duration
![image](https://github.com/TranTienProVip123/Connect-Spark-to-a-database-perform-analytics-place-review-from-google-map-in-a-Flask-app/assets/169421431/6c519d0e-cd83-407f-9a8e-4d13f1908ced)

CHAPTER III: CONCLUSION
3.1. Conclude:
- Chapter 1 introduced Spark and basic concepts related to it. We learned about Spark's architecture and components, including Spark SQL and how Spark works. We also explored databases, especially SQL and relational databases, along with practical applications of Spark. Finally, we looked at the pros and cons of Spark. This chapter has given us an overview of Spark and this powerful programming framework.
- Chapter 2 focuses on the demo part, in which we successfully connected the database to spark, stored data in mongodb, connected mongodb to spark successfully. Uploaded the Flask-App interface and visualized the evaluation using The chart, using Spark to analyze big data, gives the result of predicting the average rating.
- The project has a few shortcomings: it cannot fully utilize the power of Spark because it only uses data of about 1 million.
- In the future, we will develop more about updating more data to tens of millions or higher to take full advantage of Spark's power, research more about connecting mysql to spark, and upload it to the tkinter interface.


