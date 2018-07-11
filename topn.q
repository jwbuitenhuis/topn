pi:acos -1
xn:{$[.5>x;0-.z.s 1-x;.92>x;
 (x*2.50662823884+l*-18.61500062529+l*41.39119773534+l*-25.44106049637)%1+l*-8.47351093090+l*23.08336743743+l*-21.06224101826+3.13082909833*l:x*x-:.5;
 0.3374754822726147+l*0.9761690190917186+l*0.1607979714918209+l*0.0276438810333863+l*0.0038405729373609+l*0.0003951896511919+l*0.0000321767881768+l*0.0000002888167364+0.0000003960315187*l:log 0-log 1-x]}
nor:{$[x=2*n:x div 2;raze sqrt[-2*log n?1f]*/:(sin;cos)@\:(2*pi)*n?1f;-1_.z.s 1+x]}


topN:{[list;n]
	stats:`min`max!@[;list]peach(min;max);
	series:stats[`max]-1 2 4*(stats[`max]-stats[`min])%8;
	i:where d first where n<sum peach d:peach[list>;series];
	n#l idesc l:list i
 };

list:nor 10000000
/list:30000000?100.0
/list:100.0,(30000000#0.0)
/ \ts show 100#desc list
\ts show topN[list;100]