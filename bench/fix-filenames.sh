filenames=`ls jobfiles | egrep rq`

for file in $filenames; do
    newfilename=`echo $file | sed 's/rq-n/nodes/' \
                            | sed 's/p/-ppn/' | sed 's/\.o.*/.out/'`
    mv jobfiles/$file jobfiles/$newfilename
done

