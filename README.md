# Binary search for finding the top-n elements

An interesting question came up in the Personal KDB+ mailing list: How can you efficiently find the top 100 most expensive rides in a dataset of 1.43 billion rows?

The initial poster observed that loading the whole table into RAM, then sorting all of it was probably not the most efficient approach. A number of suggestions came up, including the idiomatic `select[100;>total_amount]` approach which will run *idesc* on a single column ('>' is k for idesc), and then using the resulting indexes to retrieve the records of interest. This is a huge improvement and likely the way to go for most scenarios.

This doesn't work directly in a partitioned database since the i column is only unique for each partition. However, *.Q.ind* allows indexing across the different partitions:

    q).Q.ind[trips](select idesc total_amount from trips)`total_amount

This will achieve most of the original goal but *idesc* still uses a fair amount of memory, enough to make a 32-bit 'home' version give up the ghost (wsfull) on a database of 10*5M rows. More egregious is the fact that *idesc* will sort every last row in the dataset, whereas we only need the binary distinction "hot or not". The 100 hotties can be sorted afterwards. (The requirements never mention sorting in the first place).

The idea is to do a binary search to find the threshold, rather than worry about the individuals. Find the domain of the dataset, start in the middle, then see how many values are larger than that. Then, depending on whether the result is greater than 100, keep going. This will be faster than an exhaustive sort if threshold comparison is cheaper than sorting, and we can limit the number of iterations. An initial test is promising:

    q)list:10000000?100.0
    q)\ts idesc list
    1588 536871072
    q)\ts list>42
    72 16777392

Arguably the simplest way to conduct a binary search is to start in the middle, measure, and then go left or right depending on the result, halving the step size on every step. The trouble is that we might be doing 10-20 iterations before closing in on the value, which would leave us with a slower algorithm. We should be able to do better, and we can, if we make basic assumptions about the data. Three basic paradigms come to mind:

 1. The data is distributed evenly
 2. The data is distributed normally
 3.  All data sits in one corner of the spectrum

### Evenly distributed
A [textbook](https://www.amazon.co.uk/Tips-Fast-Scalable-Maintainable-Kdb-ebook/dp/B00UZ8OMME/ref=sr_1_1?ie=UTF8&qid=1531091343&sr=8-1&keywords=psaris%20q%20tips) example of a KDB dataset would be:

    q)trips:flip `passenger_count`trip_distance`total_amount!10000000?/:(100;100f;100f)

The distribution of these numbers will be a flat line, as there are equal amounts of each value, especially with a large set like this. 

### Normally distributed
Borrowing from the [stat.q](https://github.com/KxSystems/kdb/blob/master/stat.q) library we can generate a random but normally distributed data set:

    q) nor:{$[x=2*n:x div  2;raze  sqrt[-2*log n?1f]*/:(sin;cos)@\:(2*pi)*n?1f;-1_.z.s  1+x]}
    q) list:nor 10000000

### All data in one corner

q) list:(10000000#0.0),100.0
 
The second idea is to use our assumption of the data set to estimate the threshold value. In an evenly or normally distributed set we can predict the threshold value with a lot of confidence, for the third case it doesn't really matter - we'd be off anyway. 

The estimate for the even set can be done as follows, essentially solving *y=ax+b*.

    stats:`min`max`count!(min;max;count)@\:list;
    estimate:stats[`min]+(stats[`max]-stats[`min])*1-n%stats[`count];

The estimate for a normal distribution is a little tricker, but we can borrow the *xn* function from stats.q which gives us the z-score for a given fraction. We can improve the estimate by including the standard deviation (dev total_amount) but appears more expensive than risking an extra iteration. Your mileage may vary. Together with the mean and standard deviation we can estimate once again.

    stats:`avg`dev`count!(avg;dev;count)@\:list;
    estimate:stats[`avg]+stats[`dev]*xn 1-n%stats[`count];

## Iterating
The next step is to double the threshold until the population is larger than 100. We're okay with more, not with less. The over adverb offers an overload that takes a predicate function. We can start with our estimate and iterate until the predicate function returns 1b. To measure the population at a given threshold we can omit the right argument for the > operator. 

    n>sum list>

This form relieves us from currying the constants n and list into the function:

    {[n;list;x] n>sum list x}[n;list]

The iterator will start with the estimate, assess the population at that threshold, and if it's smaller than the desired 100, call the iteration function which will double the distance from the max value. Since there is a possibility that the estimate is more than the max the distance is made absolute:

    q)threshold:{[top;x]top- 2*abs top-x}[max list]/[n>sum list>;estimate]

The last step is to use this threshold to harvest the actual records. We can get the indexes using *where* and apply these to the table

    q)trips where list>threshold

At this point we'll have solved the puzzle for in-memory data.

## Partitioned table
The same will work for a partitioned table with an interesting bonus. KDB+ aggressively uses multithreading when querying a partitioned database on-disk, which speeds up the algorithm considerably. Obviously the data first has to read from disk, but the below only reads the minimum. To get the statistics we can use a functional form which allows passing column names as parameters.

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


