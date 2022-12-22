cd /home/huazeng/Git/LakeSoul/
sudo rm -rf temp

cd /home/huazeng/Git/LakeSoul/aws-test
git pull

mvn clean package -DskipTests

cd ..

mkdir -p temp/work-dir
mkdir -p temp/test

cp aws-test/target/aws-test-1.0-SNAPSHOT.jar script/lakesoul.properties $LakeSoulLib/liblakesoul_io_c.so script/credentials temp/work-dir
