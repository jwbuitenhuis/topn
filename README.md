# Binary search for finding the top-n elements

An interesting question came up in the Personal KDB+ mailing list: How can you efficiently find the top 100 most expensive rides in a dataset of 1.43 billion rows?

The initial poster pointed out that loading the whole table in RAM and sorting all of it seemed very inefficient and asked for a better way. A number of suggestions came up, including the idiomatic `select[100;>total_amount]` approach. This will run *idesc* on a single column ('>' is *k* for *idesc*), and then using the resulting indexes to retrieve the records of interest. Not loading the whole table in RAM is a huge improvement and this method is normally the way to go. The syntax doesn't work directly in a partitioned database since the i column is not globally unique: it starts at zero for each partition. *.Q.ind* uses the partition counts to translate the global indexes into locals and returns the desired rows:

    q).Q.ind[trips](select idesc total_amount from trips)`total_amount

This will achieve most of the original goal but *idesc* still uses a fair amount of memory, enough to make a 32-bit 'home' version give up the ghost (wsfull) on a database of 10*5M rows. This will place a hard limit on the amount of rows that can be processed. We could run on 64-bit, but memory is still limited and data is growing fast. I'm confident we'll see databases with 20 billion taxi rides soon. Besides using a lot of memory, *idesc* will sort every last row in the dataset, whereas we only need the binary distinction "hot or not". The 100 hotties can be sorted afterwards.

## Binary threshold search
But can we do better? A scan of StackOverflow yielded terms like min-heap, priority-queues. I tried a few, like this one:

    q)list:1000000?100.0
    q)\ts t:{$[0>i:y bin z;z,y;x sublist(l#y),z,(l:i+1)_y]}[-100]/[();list]
    2409 5040

This can't be parallelised as the algorithm constantly needs the latest state to make its next decision. The memory usage is minimal, but we can certainly spare a megabyte here and there if it speeds things up. Q wasn't born to mutate small amounts of data and make a lot of decisions. It's optimised to process long homogenous lists. It's unlikely that we can sort faster than the [built-in algorithms](https://code.kx.com/q/ref/releases/ChangesIn3.5/#improved-sort-performance). What if we could avoid sorting altogether?

The idea is to look for a threshold value that only the top 100 are greater than. If we can quickly test how many values are above the threshold, we can hone in to the desired selection. The first question is whether this approach has merit in principle. An initial test looks promising:

    q)list:10000000?100.0
    q)\ts idesc list
    1588 536871072
    q)\ts list>42
    72 16777392

If we could limit the number of iterations to less than 10-20 we might win time. One approach to conducting a binary search is to start in the middle, measure, and then go left or right depending on the result, halving the step size on every step. However given the fact that we're looking for the top and we have some ideas about the data, we might be able to do better. Three basic paradigms come to mind:

 1. The data is evenly distributed
 2. The data is normally distributed
 3. The data is not distributed at all

### Evenly distributed
A [textbook](https://www.amazon.co.uk/Tips-Fast-Scalable-Maintainable-Kdb-ebook/dp/B00UZ8OMME/ref=sr_1_1?ie=UTF8&qid=1531091343&sr=8-1&keywords=psaris%20q%20tips) example of a KDB dataset would be:

    q)trips:flip `passenger_count`trip_distance`total_amount!10000000?/:(100;100f;100f)

The distribution of these numbers will be a flat line, as there are equal amounts of each value, especially with a large set like this. It follows that the estimate can be found by essentially solving *y=ax+b*.

    q)stats:`min`max`count!(min;max;count)@\:list;
    q)estimate:stats[`min]+(stats[`max]-stats[`min])*1-n%stats[`count];


### Normally distributed
Borrowing from the [stat.q](https://github.com/KxSystems/kdb/blob/master/stat.q) library we can generate a random but normally distributed data set:

    q)nor:{$[x=2*n:x div  2;raze  sqrt[-2*log n?1f]*/:(sin;cos)@\:(2*pi)*n?1f;-1_.z.s  1+x]}
    q)list:nor 10000000

The estimate is a little trickier, but we can borrow the *xn* function from stat.q which gives us the z-score for a given fraction. We can improve the estimate by weighing in the standard deviation of the data set but this appears more expensive than risking an extra iteration. Your mileage may vary. Together with the mean and standard deviation we can estimate once again:

    q)stats:`avg`dev`count!(avg;dev;count)@\:list;
    q)estimate:stats[`avg]+stats[`dev]*xn 1-n%stats[`count];


### Not distributed at all

    q)list:(10000000#0.0),100.0

There's no real value to an estimate here. All we'd need to do is make sure that the algorithm doesn't run away or takes way longer. I used this to verify the worst case scenario.
 

## Finding the threshold
I'm making the assumption that normal distributions will on average occur more often in real life scenarios. You can always plugin the even estimate if that suits you better. So now that we have a decent estimate we can start converging on a threshold value. 

Starting from our initial estimate, we can double the distance until the population is larger than 100. We're okay with more, not with less. The *over* adverb offers an overload that takes a predicate function. We can start with our estimate and iterate until the predicate function returns 0b. To measure the population at a given threshold we can omit the right argument for the > operator.

    n>sum list>

When placed within parentheses or brackets, this yields a projected function that can be called, without the currying we'd need to do since q only sees local and global scope, but not variables in the outer function:

    {[n;list;x] n>sum list x}[n;list]

The iterator will start with the estimate, assess the population at that threshold, and if it's smaller than the desired 100, call the iteration function which will move it to left, doubling the distance from the max value.

    q)threshold:{[top;x]top- 2*abs top-x}[max list]/[n>sum list>;estimate]

The last step is to use this threshold to harvest the actual records. We can get the indexes using *where* and apply these to the table

    q)trips where list>threshold

## But wait, there's less*...
At this point we'll have solved the puzzle for in-memory data, but a nice improvement since [version 3.5](https://code.kx.com/q/ref/releases/ChangesIn3.5/) is that multiple threads can read into the same memory area. Earlier versions achieved thread-safety by sending the data set in serialised form to the other thread, but now this expensive step can be avoided. This means that the floor is open for in-memory parallelism. Instead of taking it step by step, we can predict which comparisons we want run and execute them all at the same time. More vectorisation. Instead of writing decision logic and mutating values, we empathise with the machine and feed it what it likes: Long lists of neatly arranged, boring operations. In this case we can take out the converging logic.

Given 30 million floats, in the time we can iterate 3 times in series, we can blast 11 parallel compares through my 8-core machine:

    q)list:30000000?100.0
    q)\ts [list<]each 1.0*til 3
    381 100663920
    q)\ts [list<]peach 1.0*til 11
    334 1168

Ergo, why not decide on a number of thresholds to test, and test these in parallel. Then pick the one that yields the smallest result set greater than the requested amount. 

    q)peach[list<;0 1 2]
    01100011100100011101011010111000100001111010000..
    01110011111111011101111011111111100111111111011..
    11111011111111111111111111111111111111111111111..

In this case '4' would be our candidate threshold as 2015 is the smallest number greater than 100:

    q)peach[sum list>;1 2 3 4 5]
    10310258 1477697 88062 2015 23i

The heavy lifting can now be shared by all the cores and since we're at it, the same applies to the stats. On a data set of 30M records, all this shaves off another 20-30% of the running time.

    topN:{[list;n]
        stats:`min`count`max`avg!@[;list]peach(min;count;max;avg);
        est:stats[`avg]+xn 1-n%stats`count;
        series:-1_{[top;x]top- 2*abs top-x}[stats`max]\[stats[`min]<;est];
        threshold:series first where 100<peach[sum list>;series];
        n#list i idesc list i:where list>threshold
     };

From here, it was possible to optimise further by incremental profiling. On my normally distributed test set it turned out to be cheaper to make a more radical assumption about the thresholds: Measure 4/8, 2/8 and 1/8 from the right, run that in parallel and then sort whatever is left. Further optimisation included avoiding re-doing the harvesting, since we already had the information. In the below only the *min* and *max* stats, and *list>* calls add significant time:

    topN:{[list;n]
        stats:`min`max!@[;list]peach(min;max);
        series:stats[`max]-1 2 4*(stats[`max]-stats[`min])%8;
        i:where d first where n<sum peach d:peach[list>;series];
        n#l idesc l:list i
     };

On a test set of 65 million this takes under 450ms:

    q)list:nor 65000000
    q)\ts topN[list;100]
    433 25008

## Partitioned table
KDB+ is what happens when you use q to store and query data on disk. Whereas in memory some of the multithreading management falls on the programmer, when querying with KDB+ a number of important tasks are run in parallel by default. On disk data can not be changed implicitly which means mistakes or temporary changes will be cleaned up on the next *\l*. Each partition will be queried separately and the results will be stitched together cleanly. Even more impressive is the fact that map-reduce is implemented implicitly - an average is calculated by adding up all the sums and dividing by the sum of counts. The individual sums can be calculated separately.

Data is mapped into memory when the table is loaded, and only read from the device when accessed. After the first read, the file cache will be primed and subsequent access is a lot faster. This is elegant as it separate the concerns cleanly. 

All this comes at the cost 

We can use this to retrieve

Parse is useful for remembering the *"?[t;c;b;a]"* syntax:

    q)parse"select count total_amount from trips"
    ?
    `trips
    ()
    0b
    (,`total_amount)!,(#:;`total_amount)

We're after speed so we allow KDB+ to calculate the below in parallel. Since *?* returns a list of rows we extract the first one:

    stats:first ?[table;();0b;`count`avg`max!(count;avg;max),\:field];
    
The estimate can now be made in an identical way. For the comparison we're somewhat out of luck, having to fallback on currying arguments into the function, and since KDB+ doesn't implement *exec* for partitioned tables we need to do extra work to extract a boolean answer to the question "are we there yet?". Luckily this doesn't affect performance.

    cmp:{[table;field;n;est]
        n>first ?[table;enlist(>;field;est);0b;enlist[`x]!enlist(count;`i)]`x
    }[table;field;n];

Once the threshold value is approximated, we can harvest the result set. This will be somewhat larger than what we asked for. If we wanted exact results we could use the standard approach:

    q).Q.ind[trips](select idesc total_amount from topN[`trips;`total_amount])`total_amount

## Time complexity

Sorting will generally take O(n log n), and KDB+'s sorting will have been optimised to a high degree. The best case scenario for the binary threshold search will be a single compare across the whole list which will be O(n). The worst case could be 20-30 comparison iterations on top of the sort. Because the algorithm will continue to double the window, the algorithm should terminate regardless of the distribution.

How could we make this faster?


    topN:{[table;field;n]
        / estimate threshold assuming a normal distribution
        / use functional form to allow parameterization
        stats:first ?[table;();0b;`count`avg`max!(count;avg;max),\:field];
        estimate:stats[`a] + xn 1 - n % stats`c;
    
        cmp:{[table;field;n;est]
            n>first ?[table;enlist(>;field;est);0b;enlist[`x]!enlist(count;`i)]`x
        }[table;field;n];
    
        / double window to the left until happy, account for overestimation
        threshold:{[top;x]top- 2*abs top-x}[stats`m]/[cmp;est];
        / harvest
        ?[table;enlist(>;field;threshold);0b;()]
     };

## Lessons learned
Optimisation for a limited amount of use cases



* After Jeff Borror's introductory video series on Kx's YouTube channel









