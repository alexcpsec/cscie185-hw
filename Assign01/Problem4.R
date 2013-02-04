## Problem 4

## a)
smokers <- read.delim("Smokers.txt", header = TRUE, sep="\t")
class(smokers)
mode(smokers)
str(smokers)

## b)
dim(smokers)

## c)
labels(smokers)
labels(smokers)[[2]]

## d)
smokers

## e)
smokers$GDPPerCapita <- as.numeric(gsub(",","",smokers$GDPPerCapita))
str(smokers)

## f)
smokers[,c("PercentSmokes", "GDPPerCapita")]

## g)
plot(smokers$PercentSmokes, smokers$GDPPerCapita, xlab = "Percent of Smokers", ylab = "GDP Per Capita", main = "Percent of Smokers X GDP Per Capita")

## h)
bps <- c(0, 2000, 3000, 5000, 10000, 50000)
hist(smokers$GDPPerCapita, breaks=bps, main="Histogram of GDP Per Capita", xlab="GDP Per Capita", col.main = "purple")

## i)

GDP <- smokers$GDPPerCapita
labels = c("0 to 2000", "2000 to 3000", "3000 to 5000", "5000 to 10000", "10000 to 50000")
X <- c(length(GDP[GDP < 2000]),
       length(GDP[GDP >= 2000 & GDP < 3000 ]),
       length(GDP[GDP >= 3000 & GDP < 5000 ]),
       length(GDP[GDP >= 5000 & GDP < 10000 ]),
       length(GDP[GDP >= 10000 & GDP < 50000 ]))
X
pie(X, labels, clockwise=TRUE, main="Pie Chart of GDP Per Capita", col.main = "purple")
