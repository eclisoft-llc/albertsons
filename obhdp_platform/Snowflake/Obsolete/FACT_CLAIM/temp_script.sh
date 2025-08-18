# TEMP script to change the file name
cd /mapr/datalake/optum/optuminsight/p_hp2/prd/developer/p_jars/Data/CSV/FACT_CLAIM_2021_02_split

ls | while read line
do
new=`echo FACT_CLAIM_2021_02_split_${i}.csv`;
echo $new
echo $1
echo $line $new;
cp $line $new;
i=`expr $i + 1`
done

// Jan
ls | while read line
do
new=`echo FACT_CLAIM_2021_01_split_${i}.csv`;
echo $new
echo $1
echo $line $new;
cp $line $new;
i=`expr $i + 1`
done