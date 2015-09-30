echo "creating directory ..."
mkdir -p dat

echo "building models ..."
for i in `cat dev_ids.txt`; do 
	echo $i
	python run_build_models.py 5 5 $i
	sleep 1
done
