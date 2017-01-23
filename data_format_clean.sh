
#Read all the txt files from a directory
#Remove the trailing spaces 
b="_revised.csv"
for filename in *.txt; do
	IFS='.' read var1 var2 <<< $filename
	echo $filename
	sed -i '/^$/d' $filename 
	sed 's/ \{1,\}/,/g' $filename > $var1$b
	sed -i 's/f(Hz)/Freq/g' $var1$b

	
done
