# GdeltWithTupleWithPartitionWithSecondarySort
WorkInProgress

cd /home/ec2-user/finalproject/gdeltwithtuplewithSSort aws s3 cp target/gdeltwithtuple-1.0-SNAPSHOT.jar s3://xxxxxxx/gdeltwithtuplewithpartition.Jar

aws emr add-steps --cluster-id j-xxxxx --steps Type=CUSTOM_JAR,Name="Custom JAR Step",Jar=s3://xxxxxx/gdeltwithtuplewithpartition.jar,ActionOnFailure=CONTINUE,MainClass=com.oliveirf.finalproject.GdeltWithTuple,Args=s3://olivefemr/gdelt/,s3://xxxxxxx/finalprojectres --region eu-west-1
