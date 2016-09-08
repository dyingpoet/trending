library(plyr)
library(dplyr)
library(data.table)
library(rbenchmark)
library(bitops)
library(jpeg)

a3<- read.table("/Users/zzhao3/Documents/test1.txt", stringsAsFactors = FALSE, sep="\t", skip =0,  header = FALSE, comment.char = "",check.names = FALSE, quote="",
                na.strings=c("NULL","NaN", " "),fill = TRUE )
a3<-subset(a3,a3$V7!="NA")

colnames (a3) <- c("bucket", "bucket.title", "cat.child", "system.item.nbr", "catalog.item_id", "image.url", "sum.retail.test", 
          "trend.visit.test", "sum.visit.test", "impression", "click", "ctr.cap", 
          "score1", "score2", "score3", "score4", "test1", "test2", "test3", "item.title")
a3$combined <- 0/50 * a3$score1 + 0/50 * a3$score3 + 28/10 * a3$score2 + 1 * a3$score4
a3 = transform(a3, combined.rank = ave( combined, bucket, FUN = function(x) rank(-x, ties.method = "first")))

all.buckets  = array(0, dim=c(3,1980))
rownames (all.buckets) = c ("buckets", "retails" , "visits") 

all.buckets [1,] <- sort(unique(a3$bucket))
with(a3, sum(sum.retail.test[bucket==all.buckets[1,]]))

all.buckets.visit = aggregate(a3$sum.visit.test, by=list(Category=a4$bucket), FUN=sum)
all.buckets.visit.top = all.buckets.visit[all.buckets.visit$Category.item.id 
                      %in% c(1086, 1706,1285,1286,1852,1055,1946,1444, 450203, 1585,7520117, 1929, 1888, 450193,1455,1499,1514,1529,1548,8003,1501,1458), ]
all.buckets.retail = aggregate(a3$sum.retail.test, by=list(Category=a4$bucket), FUN=sum)
all.buckets.retail.top = all.buckets.retail[all.buckets.retail$Category 
                                          %in% c(1086, 1706,1285,1286,1852,1055,1946,1444, 450203, 1585,7520117, 1929, 1888), ]
a3.1 <- a3[ which(a3$bucket==1946), ]

X = array(0, dim=c(10,10,100))

base.retail = sum(a3[which(a3$test1<=3),7])
base.visit = sum(a3[which(a3$test3<=3),9])

for (i in 1:10){
  for (j in 1:10){
    for (k in 1:100){
    a3$combined <- (i-1)/50 * a3$score1 + (j-1)/50 * a3$score3 + k/10 * a3$score2 + 1 * a3$score4
   # r                         CTR              trend             visit          retail
    a3 = transform(a3, combined.rank = ave( combined, bucket, FUN = function(x) rank(-x, ties.method = "first")))
    selected.rows = which(a3$combined.rank<=3)
    X[i, j, k] = 0.5* sum(a3[selected.rows,7]) / base.retail + 0.5 * sum(a3[selected.rows,9]) / base.visit
    }
   }
}
X = X1

sum(with(a2, sum.retail.test * I(combined.rank<=3) ))


X1 = X
for (i in 1:10){
  for (j in 1:10){
    for (k in 1:100){
      if (X1[i, j, k] == max(X1)) 
      {
      d1 = i
      d2 = j
      d3 = k
      
      cat(paste("..", i,"..", j,"..", k))
      }
    }
  }
}


MM.1 = aperm(X1,c(3,2,1))
dim(MM.1)<- c(100, 100)

MM.x <- 0.1*1:nrow(MM.1)
MM.y <- 0.0003*1:ncol(MM.1)
filled.contour(MM.x, MM.y, MM.1,color = terrain.colors, plot.title = title(main = "Matrix - Contour",
  xlab = "Visit / Retail", ylab = "(CTR + Trend) / Retail"),  zlim = c(0.8067283,0.9277591),
  color.palette = colorRampPalette(c("white","grey", "green","red")), nlevels = 40,side = 1, line = 4, adj = 1, cex = .66
  )



filled.contour(x, y, MM.1, color = terrain.colors,
               plot.title = title(main = "Matrix - Contour",
                                  xlab = "Dimension1", ylab = "Dimension2"),
               plot.axes = { axis(1, seq(0, 100, by = 5))
                 axis(2, seq(0, 100, by = 10)) },
               key.title = title(main = "Score"),
               key.axes = axis(4, seq(30, 190, by = 10)))  # maybe also asp = 1
mtext(paste("filled.contour(.) from", R.version.string),
      side = 1, line = 4, adj = 1, cex = .66)


dat <- array( seq(1, 60, length.out = 60), dim=c(4, 3, 5) )

X <- aperm(dat,c(3,2,1))
dim(X)<- c(5, 12)


filled.contour(MM )




colnames(mm) <- c("top 3 retail", "top 10 retail", "top 3 trend","top 10 trend", "top 3 visit", "top 10 visit")
rownames(mm) <- c("method 1", "method 2", "method 3", "method 4", "method 5" )



for (i in 1:5) { 
mm[i,1] = with(a, sum(sum.retail.test[a[,i+13]<=3], na.rm = TRUE) ) / with(a, sum(sum.retail.test, na.rm = TRUE) )
mm[i,2] = with(a, sum(sum.retail.test[a[,i+13]<=10], na.rm = TRUE) ) / with(a, sum(sum.retail.test, na.rm = TRUE) )
mm[i,3] = with(a, sum(trend.visit.test[a[,i+13]<=3], na.rm = TRUE) ) / with(a, sum(trend.visit.test, na.rm = TRUE) )
mm[i,4] = with(a, sum(trend.visit.test[a[,i+13]<=10], na.rm = TRUE) ) / with(a, sum(trend.visit.test, na.rm = TRUE) )
mm[i,5] = with(a, sum(sum.visit.test[a[,i+13]<=3], na.rm = TRUE) ) / with(a, sum(sum.visit.test, na.rm = TRUE) )
mm[i,6] = with(a, sum(sum.visit.test[a[,i+13]<=10], na.rm = TRUE) ) / with(a, sum(sum.visit.test, na.rm = TRUE) )
}

a2 <- subset(a, score.8 != "")

rownames(a2) <- NULL

all.buckets <- sort(unique(a2$bucket))
a2["n1"] <-  a2$score.8/pmax(1,log2(a2$rank.visit))
a2["n2"] <-  a2$score.8/pmax(1,log2(a2$rank.decay.visit))
a2["n3"] <-  a2$score.8/pmax(1,log2(a2$rank.qty))
a2["n4"] <-  a2$score.8/pmax(1,log2(a2$rank.decay.qty))
a2["n5"] <-  a2$score.8/pmax(1,log2(a2$rank.retail))
a2["n6"] <-  a2$score.8/pmax(1,log2(a2$rank.decay.retail))
a2["n7"] <-  a2$score.8/pmax(1,log2(a2$rank.trend))
a2["n8"] <-  a2$score.8/pmax(1,log2(a2$rank.test))
a2["n9"] <-  a2$score.8/pmax(1,log2(a2$rank.decay.visit.year))
a2["n10"] <-  a2$score.8/pmax(1,log2(a2$rank.decay.qty.year))
a2["n11"] <-  a2$score.8/pmax(1,log2(a2$rank.decay.retail.year))

DCG1 <- function(bu) {
  with(a2, sum(n1[bucket==bu]))
}
DCG2 <- function(bu) {
  with(a2, sum(n2[bucket==bu]))
}
DCG3 <- function(bu) {
  with(a2, sum(n3[bucket==bu]))
}
DCG4 <- function(bu) {
  with(a2, sum(n4[bucket==bu]))
}
DCG5 <- function(bu) {
  with(a2, sum(n5[bucket==bu]))
}
DCG6 <- function(bu) {
  with(a2, sum(n6[bucket==bu]))
}
DCG7 <- function(bu) {
  with(a2, sum(n7[bucket==bu]))
}
DCG8 <- function(bu) {
  with(a2, sum(n8[bucket==bu]))
}

DCG9 <- function(bu) {
  with(a2, sum(n9[bucket==bu]))
}

DCG10 <- function(bu) {
  with(a2, sum(n10[bucket==bu]))
}

DCG11 <- function(bu) {
  with(a2, sum(n11[bucket==bu]))
}

## Compute total margin and number of games for each team
score1 <- sum (sapply(all.buckets, DCG1),na.rm = TRUE)/ sum( sapply(all.buckets, DCG8))
score2 <- sum (sapply(all.buckets, DCG2),na.rm = TRUE)/ sum( sapply(all.buckets, DCG8))
score3 <- sum (sapply(all.buckets, DCG3),na.rm = TRUE)/ sum( sapply(all.buckets, DCG8))
score4 <- sum (sapply(all.buckets, DCG4),na.rm = TRUE)/ sum( sapply(all.buckets, DCG8))
score5 <- sum (sapply(all.buckets, DCG5),na.rm = TRUE)/ sum( sapply(all.buckets, DCG8))
score6 <- sum (sapply(all.buckets, DCG6),na.rm = TRUE)/ sum( sapply(all.buckets, DCG8))
score7 <- sum (sapply(all.buckets, DCG7),na.rm = TRUE)/ sum( sapply(all.buckets, DCG8))
score8 <- sum (sapply(all.buckets, DCG8),na.rm = TRUE)/ sum( sapply(all.buckets, DCG8))
score9 <- sum (sapply(all.buckets, DCG9),na.rm = TRUE)/ sum( sapply(all.buckets, DCG8))
score10 <- sum (sapply(all.buckets, DCG10),na.rm = TRUE)/ sum( sapply(all.buckets, DCG8))
score11 <- sum (sapply(all.buckets, DCG11),na.rm = TRUE)/ sum( sapply(all.buckets, DCG8))
