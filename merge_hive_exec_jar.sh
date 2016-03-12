#!/bin/sh

cwd=$(cd "$(dirname "$0")"; pwd)
echo $cwd
cd $cwd

jar=$1
dir=${jar%.jar*}

rm -fr $dir && mkdir $dir
mv $jar $dir && cd $dir
jar xf $jar && mv $jar .. && cd ..

cd ./ql/target/classes/
files=`find . -type f`

cd $cwd
for i in $files
do
    echo "mv ./ql/target/classes/${i} ./${dir}/${i}"
    mv ./ql/target/classes/${i} ./${dir}/${i}
done

cd $dir && jar cf $jar *
mv $jar ../"${dir}.x.jar" && cd ..

rm -fr $dir

