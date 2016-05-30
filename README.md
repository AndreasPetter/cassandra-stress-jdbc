---+ Cassandra Performance vs. JDBC Performance

This stress-util is a quick hack to the Cassandra-Stress-util to support performance
comparisons between databases. This stuff should work with almost every JDBC database like
Oracle, Mysql, H2, Derby, ... However, I only briefly tested Oracle and Mysql.
I'll be very happy to accept pull requests...

Base version from which this is forked from was Cassandra 2.1.9.

This is a simple hack to Cassandra-stress to allow to use JDBC databases
instead of Cassandra. It is useful to compare Cassandra
to JDBC databases. To get started you will need to have maven and java 
on your path. `cassandra-stress2 help` is your friend. You will find
additional options to use this: a `-mode jdbc` to specify username and password
and `-jdbc` to specify connection parameters such as url and driver-classname.

Example for Mysql: `./cassandra-stress2 user profile=/mydiskpath/my-stress_sql.yaml ops\(insert=1\) duration=200s -mode jdbc user=root password="disclosed" -jdbc url=jdbc:mysql://mymachine/mydb driver=com.mysql.jdbc.Driver -rate threads=200`

At the moment I had only the time to make it write to JDBC databases. 

Maybe you ask yourself why nobody is publishing performance results for
major commercial databases...  

It is clear that database utilities can overwrite or delete data if not used
properly. Use at your own risk! 