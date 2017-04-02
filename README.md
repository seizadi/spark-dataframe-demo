Sample code to work on SparkSQL with data from various sources:
* synthetic data
* AWS S3 data
* RDBMS/JDBC (AWS RDS and Postgres of interest)
* Redis
* Cassandra



Follow the below steps to clone code and setup your machine.

## Spark

* Spark 2.1.0
* Scala 2.10.4

## Getting code

	git clone https://github.com/seizadi/spark-dataframe-demo.git

## Build

    mvn clean install

## IDE - IntelliJ

You can run all the examples from terminal. I have been using IntelliJ IDE for the development.
You can see from the .gitignore file that the artifacts from IDE are not checked in to GitHub.
You should be able to import this into the IDE as a Maven project.
### References

It is old but good intro to SparkSQL, some of this code here was structured
based on this earlier work:
[https://github.com/parmarsachin/spark-dataframe-demo.git](https://github.com/parmarsachin/spark-dataframe-demo.git)
[video](http://www.meetup.com/Big-Data-Developers-in-Bangalore/events/226419828/)
