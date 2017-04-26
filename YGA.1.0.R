# v 1.2
# Changes from 1.1
# dte is used instead of cdte, as appropriate
# args<-c("1","yield01","5")
# args <- c("1")
# args[1] is a flag for model building. 0=> Build Model, 1=> Backtest 2=> Backtest and BootStrap
# args[2] is the strategy name
# args[3] is the redisdatabase
# args[4] is the date

library(caret)
library(nnet)
library(doParallel)
library(RcppRoll)
library(TTR)
library(rredis)
library(PerformanceAnalytics)
library(log4r)
library(RTrade)
library(zoo)
library(RQuantLib)
library(jsonlite)
library(RcppRoll)
options(scipen = 999)

args.commandline=commandArgs(trailingOnly=TRUE)
if(length(args.commandline)>0){
        args<-args.commandline
}

redisConnect()
redisSelect(1)
if(length(args)>1){
        static<-redisHGetAll(toupper(args[2]))
}else{
        static<-redisHGetAll("YIELD03")
}

newargs<-unlist(strsplit(static$args,","))
if(length(args)<=1 && length(newargs>1)){
        args<-newargs
}
redisClose()
today=strftime(Sys.Date(),tz=kTimeZone,format="%Y-%m-%d")

kWriteToRedis <- as.logical(static$WriteToRedis)
kGetMarketData<-as.logical(static$GetMarketData)
kBackTestCutOffDate<-static$BackTestCutOffDate
kBackTestStartDate<-static$BackTestStartDate
kBackTestEndDate<-static$BackTestEndDate
kBackTestEndDate = ifelse(length(args) <= 1, kBackTestEndDate, today)
kFNODataFolder <- static$FNODataFolder
kNiftyDataFolder <- static$NiftyDataFolder
kTimeZone <- static$TimeZone
kIndex<-static$Index
kMLFile<-static$MLFile
kBrokerage<-as.numeric(static$SingleLegBrokerageAsPercentOfValue)/100
kPerContractBrokerage=as.numeric(static$SingleLegBrokerageAsValuePerContract)
kContractSize=as.numeric(static$ContractSize)
kMaxContracts=as.numeric(static$MaxContracts)
kHomeDirectory=static$HomeDirectory
kExchangeMargin=as.numeric(static$ExchangeMargin)
kTimeDecayRisk=as.numeric(static$TimeDecayRisk)
kTimeDecayExitRisk=as.numeric(static$TimeDecayExitRisk)
kVegaRisk=as.numeric(static$VegaRisk)
kThetaRisk=as.numeric(static$ThetaRisk)
kEntryVolThreshold=as.numeric(static$EntryVolThreshold)
kReturnThresholdFullSize=as.numeric(static$ReturnThresholdFullSize)
kReturnThresholdHalfSize=as.numeric(static$ReturnThresholdHalfSize)
kTPThreshold=as.numeric(static$TPThreshold)
kHistoricalVol=as.numeric(static$HistoricalVol)
kSDEntry=as.numeric(static$SDEntry)
kSDExit=as.numeric(static$SDExit)
kRiskUnits=as.numeric(static$RiskUnits)
kMaxLongPosition=as.numeric(static$MaxLongPos)
kMaxShortPosition=as.numeric(static$MaxShortPos)
kRiskUnitSize=kMaxLongPosition+kMaxShortPosition
kMaxLongPosition=kMaxLongPosition*kRiskUnits
kMaxShortPosition=kMaxShortPosition*kRiskUnits
kMaxTotalPosition=kRiskUnits*kRiskUnitSize
kFullSize=as.numeric(static$FullSize)*kRiskUnits
kHalfSize=as.numeric(static$HalfSize)*kRiskUnits
kLogFile=static$LogFile
ptm <- proc.time()
kBackTestEndDate=strftime(adjust("India",as.Date(kBackTestEndDate, tz = kTimeZone),bdc=2),"%Y-%m-%d")
kBackTestStartDate=strftime(adjust("India",as.Date(kBackTestStartDate, tz = kTimeZone),bdc=0),"%Y-%m-%d")

logger <- create.logger()
logfile(logger) <- kLogFile
level(logger) <- 'INFO'
###### BACKTEST ##############

###### Load Data #############
load(paste(kNiftyDataFolder, "NSENIFTY.Rdata", sep = ""))
niftymd <- md
rm(md)
if (length(args) > 1) {
        # we have ohlc for today. Add to nsenifty
        newrow = getPriceArrayFromRedis(9,
                                        "NSENIFTY_IND___",
                                        "tick",
                                        "close",
                                        paste(today, " 09:12:00"),paste(today, " 15:30:00"))
        if(nrow(newrow)==1){
                newrow <-
                        data.frame(
                                "symbol" = "NSENIFTY",
                                "date" = newrow$date[1],
                                "open" = newrow$open[1],
                                "high" = newrow$high[1],
                                "low" = newrow$low[1],
                                "close" = newrow$close[1],
                                "settle" = newrow$close[1],
                                "volume" = 0,
                                "aopen" = newrow$open[1],
                                "ahigh" = newrow$high[1],
                                "alow" = newrow$low[1],
                                "aclose" = newrow$close[1],
                                "asettle" = newrow$close[1],
                                "avolume" = 0,
                                "splitadjust" = 1
                        )
                
                niftymd <- rbind(niftymd, newrow)                
        }
}
niftymd <- unique(niftymd) # remove duplicate rows
load(paste("fit", "NSENIFTY", "swing02", "v1.0.Rdata", sep =
                   "_"))
change <- diff(log(niftymd$settle)) * 100
rollingsd <- roll_sd(change, 252)
rollingsd <-
        c(rep(NA_character_, (nrow(niftymd) - length(rollingsd))), rollingsd)
niftymd$rollingsd <- as.numeric(rollingsd)
niftymd$rollingsd <- kHistoricalVol


##### 1. Calculate Indicators ########

trend <-
        Trend(niftymd$date, niftymd$high, niftymd$low, niftymd$settle)
niftymd$trend <- trend$trend
sd <- roll_sd(niftymd$settle, 10) * sqrt(9 / 10)
NA9Vec <- rep(NA, 9)
sd <- c(NA9Vec, sd)
niftymd$closezscore <-
        (niftymd$settle - SMA(niftymd$settle, 10)) / sd
niftymd$highzscore <-
        (niftymd$high - SMA(niftymd$high, 10)) / c(NA9Vec, roll_sd(niftymd$high, 10) * sqrt(9 /
                                                                                                    10))
niftymd$lowzscore <-
        (niftymd$low - SMA(niftymd$low, 10)) / c(NA9Vec, roll_sd(niftymd$low, 10) * sqrt(9 / 10))
ma <- SMA(niftymd$settle, 10)
niftymd$mazscore <-
        (ma - SMA(ma, 10)) / c(NA9Vec, roll_sd(ma, 10) * sqrt(9 / 10))
daysinupswing = BarsSince(trend$updownbar <= 0)

daysindownswing = BarsSince(trend$updownbar >= 0)

niftymd$swing = ifelse(daysinupswing > 0, 1, 0)

niftymd$daysinuptrend = BarsSince(trend$trend <= 0)

niftymd$daysindowntrend = BarsSince(trend$trend >= 0)

niftymd$daysintrend = ifelse(
        trend$trend == 1,
        niftymd$daysinuptrend,
        ifelse(trend$trend == -1, niftymd$daysindowntrend, 0)
)

niftymd$daysinswing = daysinupswing + daysindownswing

niftymd$atr <- ATR(niftymd[, c("high", "low", "close")], 10)[, 2]
niftymd <- na.omit(niftymd)
trainingsize = sum(niftymd$date < "2013-01-01")
a <-
        (niftymd$daysintrend - mean(niftymd$daysintrend[niftymd$date < "2013-01-01"])) / (3 * sd(niftymd$daysintrend[niftymd$date <
                                                                                                                             "2013-01-01"]) * sqrt((trainingsize - 1) / trainingsize) / (2 * pi))
niftymd$softmax_daysintrend <- 1 / (1 + exp(-a))
b <-
        (niftymd$daysinswing - mean(niftymd$daysinswing[niftymd$date < "2013-01-01"])) / (3 * sd(niftymd$daysinswing[niftymd$date <
                                                                                                                             "2013-01-01"]) * sqrt((trainingsize - 1) / trainingsize) / (2 * pi))
niftymd$softmax_daysinswing <- 1 / (1 + exp(-b))
niftymd$dayreturn <-
        (log(niftymd$settle) - log(Ref(niftymd$settle,-1))) *
        100

####### 2. Generate Buy/Sell Arrays ##########

niftymd$predict.raw <- predict(fit, niftymd, type = 'prob')
niftymd$predict.class <- predict(fit, niftymd)

niftymd$buy = as.numeric(niftymd$predict.class) == 2
niftymd$sell = as.numeric(niftymd$predict.class) == 1
niftymd$short = as.numeric(niftymd$predict.class) == 1
niftymd$cover = as.numeric(niftymd$predict.class) == 2

niftymd$buy = ExRem(niftymd$buy, niftymd$sell)

niftymd$sell = ExRem(niftymd$sell, niftymd$buy)

niftymd$short = ExRem(niftymd$short, niftymd$cover)

niftymd$cover = ExRem(niftymd$cover, niftymd$short)


niftymd$buyprice <- niftymd$settle

niftymd$sellprice <- niftymd$settle

niftymd$shortprice <- niftymd$settle

niftymd$coverprice <- niftymd$settle


niftymd$inlongtrade <- Flip(niftymd$buy, niftymd$sell)
niftymd$inshorttrade <- Flip(niftymd$short, niftymd$cover)

###### 3. Create SL/TP Array ############
niftymd$stoplosslevel = 0

niftymd <-
        niftymd[niftymd$date >= as.POSIXct(kBackTestStartDate, tz = kTimeZone), ]
niftymd <-
        niftymd[niftymd$date <= as.POSIXct(kBackTestEndDate, tz = kTimeZone), ]

# Remove bad business days
tempdates<-seq(from=as.Date(kBackTestStartDate,tz=kTimeZone),to=as.Date(min(Sys.Date(),as.Date(kBackTestEndDate,tz=kTimeZone))),by=1)
bizdays<-isBusinessDay("India",tempdates)
validindices<-match(as.Date(niftymd$date,tz=kTimeZone),tempdates[bizdays])
validindices<-complete.cases(validindices)
niftymd<-niftymd[validindices,]

##### Calculate ATM Strike ######
expirydate = as.Date(sapply(niftymd$date, getExpiryDate), tz = kTimeZone)
niftymd$expirydate <- expirydate

nextexpiry = as.Date(sapply(
        as.Date(niftymd$expirydate + 20, tz = kTimeZone),
        getExpiryDate
), tz = kTimeZone)
niftymd$contractexpiry = as.Date(ifelse(
        businessDaysBetween("India",as.Date(niftymd$date, tz = kTimeZone),niftymd$expirydate) <= 3,
        nextexpiry,
        niftymd$expirydate
),
tz = kTimeZone)
niftymd$futuresettle = -1
#update future price for contractexpiry
for (i in 1:nrow(niftymd)) {
        #update futures data for contract expiry
        expiry = format(niftymd[i, c("contractexpiry")], format = "%Y%m%d")
        symbol = paste("NSENIFTY", "FUT", expiry, "", "", sep = "_")
        load(paste(kFNODataFolder, expiry,"/",symbol, ".Rdata", sep = ""))
        datarow = md[md$date == niftymd$date[i], ]
        if (length(args) > 1 &&
            nrow(datarow) == 0 &&
            niftymd$date[i] == as.POSIXct(today, "%Y-%m-%d", tz = kTimeZone)) {
                datarow = getPriceArrayFromRedis(9,
                                                 symbol,
                                                 "tick",
                                                 "close",
                                                 paste(today, " 09:12:00"),paste(today, " 15:30:00"))
        }
        niftymd$futuresettle[i] = datarow$settle[1]
        niftymd$futurehigh[i] = datarow$high[1]
        niftymd$futurelow[i] = datarow$low[1]
        niftymd$futureopen[i] = datarow$open[1]
        
        #update futures data for current expiry date
        expiry = format(niftymd[i, c("expirydate")], format = "%Y%m%d")
        symbol = paste("NSENIFTY", "FUT", expiry, "", "", sep = "_")
        load(paste(kFNODataFolder, expiry,"/",symbol, ".Rdata", sep = ""))
        datarow = md[md$date == niftymd$date[i], ]
        if (length(args) > 1 &&
            nrow(datarow) == 0 &&
            niftymd$date[i] == as.POSIXct(today, "%Y-%m-%d", tz = kTimeZone)) {
                datarow = getPriceArrayFromRedis(9,
                                                 symbol,
                                                 "tick",
                                                 "close",
                                                 paste(today, " 09:12:00"),paste(today, " 15:30:00"))
        }
        niftymd$currentfuturesettle[i] = datarow$settle[1]
        niftymd$currentfuturehigh[i] = datarow$high[1]
        niftymd$currentfuturelow[i] = datarow$low[1]
        niftymd$currentfutureopen[i] = datarow$open[1]
        
}
niftymd$ATMStrike = round(niftymd$futuresettle / 100) * 100
niftymd$dte <-
        businessDaysBetween("India",
                            as.Date(niftymd$date, tz = kTimeZone),
                            niftymd$contractexpiry)
niftymd$cushionentry <- sqrt(niftymd$dte) * niftymd$rollingsd * kSDEntry
niftymd$cushionexit <- sqrt(niftymd$dte) * niftymd$rollingsd * kSDExit
niftymd$callstrike <-
        round((
                niftymd$futuresettle + niftymd$futuresettle * niftymd$cushionentry / 100
        ) / 100) * 100
niftymd$putstrike <-
        round((
                niftymd$futuresettle - niftymd$futuresettle * niftymd$cushionentry / 100
        ) / 100) * 100
#niftymd$callstrikesl<-niftymd$callstrike-sqrt(as.integer(niftymd$contractexpiry-as.Date(niftymd$date,tz=kTimeZone)))*niftymd$rollingsd*kSDExit*niftymd$callstrike/100
#niftymd$putstrikesl<-niftymd$putstrike+sqrt(as.integer(niftymd$contractexpiry-as.Date(niftymd$date,tz=kTimeZone)))*niftymd$rollingsd*kSDExit*niftymd$putstrike/100


niftymd <- na.omit(niftymd)

niftymd$shortprice = -1
niftymd$putcoverprice = 0
niftymd$callcoverprice = 0
niftymd$cdte = -1

niftymd$incalltrade = 0
niftymd$inputtrade = 0
niftymd$callprice = -1
niftymd$putprice = -1
niftymd$callentryvol = -1
niftymd$putentryvol = -1
niftymd$callexitvol = -1
niftymd$putentryvol = -1
niftymd$callentrydelta = -1
niftymd$callentrytheta = -1
niftymd$callentryvega = -1
niftymd$putentrydelta = -1
niftymd$putentrytheta = -1
niftymd$putentryvega = -1
niftymd$callexittheta = 0
niftymd$callexitvega = -1
niftymd$callexitdelta = -1
niftymd$putexittheta = 0
niftymd$putexitvega = -1
niftymd$putexitdelta = -1
niftymd$putexitdate = NA_character_
niftymd$callexitdate = NA_character_
niftymd$callexitreason = NA_character_
niftymd$putexitreason = NA_character_
niftymd$underlyingcallexitprice = -1
niftymd$underlyingputexitprice = -1


#Calculate Entry
for (i in 1:nrow(niftymd)) {
        niftymd$cdte[i] <-
                as.integer(niftymd$contractexpiry[i] - as.Date(niftymd$date[i], tz = kTimeZone))
        side = "UNDEFINED"
        strike = 0
        if (niftymd[i, c("inshorttrade")] == 1) {
                side = "CALL"
                strike = niftymd[i, c("callstrike")]
        } else if (niftymd[i, c("inlongtrade")] == 1) {
                side = "PUT"
                strike = niftymd[i, c("putstrike")]
        }
        
        if (side == "CALL") {
                strike = niftymd$callstrike[i]
                expiry = format(niftymd$contractexpiry[i], format = "%Y%m%d")
                symbol = paste("NSENIFTY", "OPT", expiry, side, strike, sep = "_")
                load(paste(kFNODataFolder,expiry,"/", symbol, ".Rdata", sep = ""))
                datarow = md[md$date == niftymd$date[i],]
                if (length(args) > 1 &&
                    nrow(datarow) == 0 &&
                    niftymd$date[i] == as.POSIXct(today, "%Y-%m-%d", tz = kTimeZone)) {
                        datarow = getPriceArrayFromRedis(9,
                                                         symbol,
                                                         "tick",
                                                         "close",
                                                         paste(today, " 09:12:00"),paste(today, " 15:30:00"))
                }
                if (nrow(datarow) == 1) {
                        niftymd$callprice[i] = datarow$settle
                        niftymd$callentryvol[i] <-
                                EuropeanOptionImpliedVolatility(
                                        tolower(side),
                                        niftymd$callprice[i],
                                        niftymd$futuresettle[i],
                                        strike,
                                        0.015,
                                        0.06,
                                        niftymd$dte[i] / 365,
                                        0.1
                                )
                        greeks <-
                                EuropeanOption(
                                        tolower(side),
                                        niftymd$futuresettle[i],
                                        strike,
                                        0.015,
                                        0.06,
                                        niftymd$dte[i] / 365,
                                        niftymd$callentryvol[i]
                                )
                        niftymd$callentrydelta[i] <- greeks$delta
                        niftymd$callentrytheta[i] <-
                                greeks$theta * (1 / 365)
                        niftymd$callentryvega[i] <-
                                greeks$vega / 100
                }
        }
        
        if (side == "PUT") {
                strike = niftymd$putstrike[i]
                expiry = format(niftymd$contractexpiry[i], format = "%Y%m%d")
                symbol = paste("NSENIFTY", "OPT", expiry, side, strike, sep = "_")
                load(paste(kFNODataFolder, expiry,"/",symbol, ".Rdata", sep = ""))
                datarow = md[md$date == niftymd$date[i],]
                if (length(args) > 1 &&
                    nrow(datarow) == 0 &&
                    niftymd$date[i] == as.POSIXct(today, "%Y-%m-%d", tz = kTimeZone)) {
                        datarow = getPriceArrayFromRedis(9,
                                                         symbol,
                                                         "tick",
                                                         "close",
                                                         paste(today, " 09:12:00"),paste(today, " 15:30:00"))
                }
                if (nrow(datarow) == 1) {
                        niftymd$putprice[i] = datarow$settle
                        niftymd$putentryvol[i] <-
                                EuropeanOptionImpliedVolatility(
                                        tolower(side),
                                        niftymd$putprice[i],
                                        niftymd$futuresettle[i],
                                        strike,
                                        0.015,
                                        0.06,
                                        niftymd$dte[i] / 365,
                                        0.1
                                )
                        greeks <-
                                EuropeanOption(
                                        tolower(side),
                                        niftymd$futuresettle[i],
                                        strike,
                                        0.015,
                                        0.06,
                                        niftymd$dte[i] / 365,
                                        niftymd$putentryvol[i]
                                )
                        niftymd$putentrydelta[i] <- greeks$delta
                        niftymd$putentrytheta[i] <-
                                greeks$theta * (1 / 365)
                        niftymd$putentryvega[i] <-
                                greeks$vega / 100
                }
        }
}

for (i in 1:nrow(niftymd)) {
        if (niftymd$callentrytheta[i] / niftymd$callentryvega[i] < kTimeDecayRisk) {
                niftymd$incalltrade[i] = 1
        }
        if (niftymd$putentrytheta[i] / niftymd$putentryvega[i] < kTimeDecayRisk) {
                niftymd$inputtrade[i] = 1
        }
}

#Calculate Exit
niftymd$exitdate = NA_character_
for (i in 1:nrow(niftymd)) {
        strike = 0
        side = "UNDEFINED"
        if (niftymd[i, c("inshorttrade")] == 1) {
                side = "CALL"
                strike = niftymd[i, c("callstrike")]
                
                # check for exit levels till j rows ahead
                lookaheadindex = which(
                        as.Date(niftymd$date, tz = kTimeZone) == niftymd$contractexpiry[i]
                )
                if (length(lookaheadindex) == 0 &&
                    niftymd$contractexpiry[i] == as.Date("2014-04-24")) {
                        #contract expiry was changed on account of elections, but contract expiry in db is still 2014-04-24
                        lookaheadindex = which(
                                as.Date(niftymd$date, tz = kTimeZone) == as.Date("2014-04-23", tz =
                                                                                              kTimeZone)
                        )
                        
                }
                if (length(lookaheadindex) == 0) {
                        # not reached contractexpiry
                        lookaheadindex = nrow(niftymd)
                }
                if ((i + 1) <= lookaheadindex) {
                        for (j in (i + 1):lookaheadindex) {
                                if (is.na(niftymd$callexitdate[i])) {
                                        start = paste(
                                                format(
                                                        niftymd$date[j],
                                                        format = "%Y-%m-%d"
                                                ),
                                                " 00:00:00",
                                                sep = ""
                                        )
                                        end = paste(
                                                format(
                                                        niftymd$date[j],
                                                        format = "%Y-%m-%d"
                                                ),
                                                " 00:00:01",
                                                sep = ""
                                        )
                                        expiry = format(niftymd[i, c("contractexpiry")], format =
                                                                "%Y%m%d")
                                        symbol = paste(
                                                "NSENIFTY",
                                                "OPT",
                                                expiry,
                                                side,
                                                strike,
                                                sep = "_"
                                        )
                                        load(
                                                paste(
                                                        kFNODataFolder,
                                                        expiry,"/",
                                                        symbol,
                                                        ".Rdata",
                                                        sep = ""
                                                )
                                        )
                                        datarow = md[md$date == niftymd$date[j], ]
                                        if (length(args) > 1 &&
                                            nrow(datarow) == 0 &&
                                            niftymd$date[i] == as.POSIXct(today,
                                                                          "%Y-%m-%d",
                                                                          tz = kTimeZone)) {
                                                datarow = getPriceArrayFromRedis(
                                                        9,
                                                        symbol,
                                                        "tick",
                                                        "close",
                                                        paste(
                                                                today,
                                                                " 09:12:00"
                                                        ),paste(today, " 15:30:00")
                                                )
                                        }
                                        if (nrow(datarow) == 1) {
                                                optionprice = datarow$settle[1]
                                                cdte <-
                                                        as.integer(
                                                                niftymd$contractexpiry[i] - as.Date(
                                                                        niftymd$date[j],
                                                                        tz = kTimeZone
                                                                )
                                                        )
                                                print(paste(
                                                        "i:",
                                                        i,
                                                        ",j:",
                                                        j,
                                                        sep = ""
                                                ))
                                                futuresettle = ifelse(
                                                        niftymd$contractexpiry[j] == niftymd$contractexpiry[i],
                                                        niftymd$futuresettle[j],
                                                        niftymd$currentfuturesettle[j]
                                                )
                                                futureopen = ifelse(
                                                        niftymd$contractexpiry[j] == niftymd$contractexpiry[i],
                                                        niftymd$futureopen[j],
                                                        niftymd$currentfutureopen[j]
                                                )
                                                futurehigh = ifelse(
                                                        niftymd$contractexpiry[j] == niftymd$contractexpiry[i],
                                                        niftymd$futurehigh[j],
                                                        niftymd$currentfuturehigh[j]
                                                )
                                                futurelow = ifelse(
                                                        niftymd$contractexpiry[j] == niftymd$contractexpiry[i],
                                                        niftymd$futurelow[j],
                                                        niftymd$currentfuturelow[j]
                                                )
                                                dte=ifelse(niftymd$contractexpiry[j]==niftymd$contractexpiry[i],niftymd$dte[j],
                                                           businessDaysBetween("India", as.Date(niftymd$date[j], tz = kTimeZone),niftymd$expirydate[j]))
                                                #futureopen=ifelse(niftymd$expirydate[j]==niftymd[i,c("contractexpiry")],niftymd$currentfutureopen[j],niftymd$futureopen[j])
                                                #futurehigh=ifelse(niftymd$expirydate[j]==niftymd[i,c("contractexpiry")],niftymd$currentfuturehigh[j],niftymd$futurehigh[j])
                                                #futurelow=ifelse(niftymd$expirydate[j]==niftymd[i,c("contractexpiry")],niftymd$currentfuturelow[j],niftymd$futurelow[j])
                                                vol=0
                                                if(dte>0){
                                                        vol <-
                                                                EuropeanOptionImpliedVolatility(
                                                                        tolower(side),
                                                                        optionprice,
                                                                        futuresettle,
                                                                        strike,
                                                                        0.015,
                                                                        0.06,
                                                                        dte / 365,
                                                                        0.1
                                                                )                                                        
                                                }

                                                greeks <-
                                                        EuropeanOption(
                                                                tolower(side),
                                                                futuresettle,
                                                                strike,
                                                                0.015,
                                                                0.06,
                                                                dte / 365,
                                                                vol
                                                        )
                                                niftymd$callexitdelta[i] =
                                                        greeks$delta
                                                niftymd$callexittheta[i] =
                                                        greeks$theta * (1 / 365)
                                                niftymd$callexitvega[i] =
                                                        greeks$vega / 100
                                                # sl = sqrt(
                                                #         as.integer(
                                                #                 niftymd$contractexpiry[i] - as.Date(
                                                #                         niftymd$date[j],
                                                #                         tz = kTimeZone
                                                #                 )
                                                #         )
                                                # ) * niftymd$rollingsd[i] * kSDExit * strike / 100
                                                sl=sqrt(dte)* niftymd$rollingsd[i] * kSDExit * strike / 100
                                                if (side == "CALL") {
                                                        #check sl
                                                        slprice = strike -
                                                                sl
                                                        stop = niftymd$futurehigh[j] >
                                                                slprice
                                                        if (stop) {
                                                                coverprice = ifelse(
                                                                        futureopen > slprice,
                                                                        futureopen,
                                                                        slprice
                                                                )
                                                                niftymd$callexitdate[i] =
                                                                        paste(
                                                                                format(
                                                                                        niftymd$date[j],
                                                                                        format = "%Y-%m-%d"
                                                                                ),
                                                                                sep = ""
                                                                        )
                                                                if (as.Date(
                                                                        niftymd$callexitdate[i],
                                                                        tz = kTimeZone
                                                                ) == niftymd$contractexpiry[i]) {
                                                                        niftymd$callcoverprice[i] = datarow$close[1]
                                                                } else{
                                                                        greekssl <-
                                                                                EuropeanOption(
                                                                                        tolower(
                                                                                                side
                                                                                        ),
                                                                                        coverprice,
                                                                                        strike,
                                                                                        0.015,
                                                                                        0.06,
                                                                                        dte / 365,
                                                                                        vol
                                                                                )
                                                                        niftymd$callcoverprice[i] =
                                                                                greekssl$value
                                                                        
                                                                }
                                                                niftymd$callexitreason[i] =
                                                                        "STOPLOSS"
                                                                niftymd$underlyingcallexitprice[i] = coverprice
                                                        } else{
                                                                #check tp
                                                                tp=TRUE
                                                                if(cdte>0){
                                                                        tp = optionprice *
                                                                                365 / (
                                                                                        futurelow * 0.1 * cdte
                                                                                ) < kTPThreshold / 100                                                                        
                                                                }

                                                                if (tp) {
                                                                        niftymd$callcoverprice[i] = (
                                                                                0.1 * futuresettle * kTPThreshold * cdte
                                                                        ) / 36500
                                                                        niftymd$callexitdate[i] =
                                                                                paste(
                                                                                        format(
                                                                                                niftymd$date[j],
                                                                                                format = "%Y-%m-%d"
                                                                                        ),
                                                                                        sep = ""
                                                                                )
                                                                        niftymd$callexitreason[i] =
                                                                                "TAKEPROFIT"
                                                                        niftymd$underlyingcallexitprice[i] = futuresettle
                                                                        
                                                                } else if (niftymd$callexittheta[i] /
                                                                           niftymd$callexitvega[i] > kTimeDecayExitRisk) {
                                                                        niftymd$callcoverprice[i] = datarow$settle[i]
                                                                        niftymd$callexitdate[i] =
                                                                                paste(
                                                                                        format(
                                                                                                niftymd$date[j],
                                                                                                format = "%Y-%m-%d"
                                                                                        ),
                                                                                        sep = ""
                                                                                )
                                                                        niftymd$callexitreason[i] =
                                                                                "RISKEXIT"
                                                                        niftymd$underlyingcallexitprice[i] =
                                                                                futuresettle
                                                                }
                                                                
                                                        }
                                                }
                                                
                                        }
                                        
                                }
                        }
                }
        }
        
        if (niftymd[i, c("inlongtrade")] == 1) {
                side = "PUT"
                strike = niftymd[i, c("putstrike")]
                
                # check for exit levels till j rows ahead
                lookaheadindex = which(
                        as.Date(niftymd$date, tz = kTimeZone) == niftymd$contractexpiry[i]
                )
                if (length(lookaheadindex) == 0 &&
                    niftymd$contractexpiry[i] == as.Date("2014-04-24")) {
                        #contract expiry was changed on account of elections, but contract expiry in db is still 2014-04-24
                        lookaheadindex = which(
                                as.Date(niftymd$date, tz = kTimeZone) == as.Date("2014-04-23", tz =
                                                                                              kTimeZone)
                        )
                        
                }
                if (length(lookaheadindex) == 0) {
                        # not reached contractexpiry
                        lookaheadindex = nrow(niftymd)
                }
                if ((i + 1) <= lookaheadindex) {
                        for (j in (i + 1):lookaheadindex) {
                                if (is.na(niftymd$putexitdate[i])) {
                                        start = paste(
                                                format(
                                                        niftymd$date[j],
                                                        format = "%Y-%m-%d"
                                                ),
                                                " 00:00:00",
                                                sep = ""
                                        )
                                        end = paste(
                                                format(
                                                        niftymd$date[j],
                                                        format = "%Y-%m-%d"
                                                ),
                                                " 00:00:01",
                                                sep = ""
                                        )
                                        expiry = format(niftymd[i, c("contractexpiry")], format =
                                                                "%Y%m%d")
                                        symbol = paste(
                                                "NSENIFTY",
                                                "OPT",
                                                expiry,
                                                side,
                                                strike,
                                                sep = "_"
                                        )
                                        load(
                                                paste(
                                                        kFNODataFolder,
                                                        expiry,"/",
                                                        symbol,
                                                        ".Rdata",
                                                        sep = ""
                                                )
                                        )
                                        datarow = md[md$date == niftymd$date[j], ]
                                        if (length(args) > 1 &&
                                            nrow(datarow) == 0 &&
                                            niftymd$date[i] == as.POSIXct(today,
                                                                          "%Y-%m-%d",
                                                                          tz = kTimeZone)) {
                                                datarow = getPriceArrayFromRedis(
                                                        9,
                                                        symbol,
                                                        "tick",
                                                        "close",
                                                        paste(
                                                                today,
                                                                " 09:12:00"
                                                        ),paste(today, " 15:30:00")
                                                )
                                        }
                                        if (nrow(datarow) == 1) {
                                                optionprice = datarow$settle[1]
                                                cdte <-
                                                        as.integer(
                                                                niftymd$contractexpiry[i] - as.Date(
                                                                        niftymd$date[j],
                                                                        tz = kTimeZone
                                                                )
                                                        )
                                                print(paste(
                                                        "i:",
                                                        i,
                                                        ",j:",
                                                        j,
                                                        sep = ""
                                                ))
                                                # futuresettle=ifelse(niftymd$expirydate[j]==niftymd[i,c("contractexpiry")],niftymd$currentfuturesettle[j],niftymd$futuresettle[j])
                                                # futureopen=ifelse(niftymd$expirydate[j]==niftymd[i,c("contractexpiry")],niftymd$currentfutureopen[j],niftymd$futureopen[j])
                                                # futurehigh=ifelse(niftymd$expirydate[j]==niftymd[i,c("contractexpiry")],niftymd$currentfuturehigh[j],niftymd$futurehigh[j])
                                                # futurelow=ifelse(niftymd$expirydate[j]==niftymd[i,c("contractexpiry")],niftymd$currentfuturelow[j],niftymd$futurelow[j])
                                                futuresettle = ifelse(
                                                        niftymd$contractexpiry[j] == niftymd$contractexpiry[i],
                                                        niftymd$futuresettle[j],
                                                        niftymd$currentfuturesettle[j]
                                                )
                                                futureopen = ifelse(
                                                        niftymd$contractexpiry[j] == niftymd$contractexpiry[i],
                                                        niftymd$futureopen[j],
                                                        niftymd$currentfutureopen[j]
                                                )
                                                futurehigh = ifelse(
                                                        niftymd$contractexpiry[j] == niftymd$contractexpiry[i],
                                                        niftymd$futurehigh[j],
                                                        niftymd$currentfuturehigh[j]
                                                )
                                                futurelow = ifelse(
                                                        niftymd$contractexpiry[j] == niftymd$contractexpiry[i],
                                                        niftymd$futurelow[j],
                                                        niftymd$currentfuturelow[j]
                                                )
                                                dte=ifelse(niftymd$contractexpiry[j]==niftymd$contractexpiry[i],niftymd$dte[j],
                                                           businessDaysBetween("India", as.Date(niftymd$date[j], tz = kTimeZone),niftymd$expirydate[j]))
                                                vol<-0
                                                if(dte>0){
                                                        vol <-
                                                                EuropeanOptionImpliedVolatility(
                                                                        tolower(side),
                                                                        optionprice,
                                                                        futuresettle,
                                                                        strike,
                                                                        0.015,
                                                                        0.06,
                                                                        dte / 365,
                                                                        0.1
                                                                )
                                                }

                                                greeks <-
                                                        EuropeanOption(
                                                                tolower(side),
                                                                futuresettle,
                                                                strike,
                                                                0.015,
                                                                0.06,
                                                                dte / 365,
                                                                vol
                                                        )
                                                niftymd$putexitdelta[i] =
                                                        greeks$delta
                                                niftymd$putexittheta[i] =
                                                        greeks$theta * (1 / 365)
                                                niftymd$putexitvega[i] =
                                                        greeks$vega / 100
                                                # sl = sqrt(
                                                #         as.integer(
                                                #                 niftymd$contractexpiry[i] - as.Date(
                                                #                         niftymd$date[j],
                                                #                         tz = kTimeZone
                                                #                 )
                                                #         )
                                                # ) * niftymd$rollingsd[i] * kSDExit * strike / 100
                                                sl=sqrt(dte)* niftymd$rollingsd[i] * kSDExit * strike / 100
                                                if (side == "PUT") {
                                                        #check sl
                                                        slprice = strike +
                                                                sl
                                                        stop = niftymd$futurelow[j] <
                                                                slprice
                                                        if (stop) {
                                                                coverprice = ifelse(
                                                                        futureopen < slprice,
                                                                        futureopen,
                                                                        slprice
                                                                )
                                                                niftymd$putexitdate[i] =
                                                                        paste(
                                                                                format(
                                                                                        niftymd$date[j],
                                                                                        format = "%Y-%m-%d"
                                                                                ),
                                                                                sep = ""
                                                                        )
                                                                if (as.Date(
                                                                        niftymd$putexitdate[i],
                                                                        tz = kTimeZone
                                                                ) == niftymd$contractexpiry[i]) {
                                                                        niftymd$putcoverprice[i] = datarow$close[1]
                                                                } else{
                                                                        greekssl <-
                                                                                EuropeanOption(
                                                                                        tolower(
                                                                                                side
                                                                                        ),
                                                                                        coverprice,
                                                                                        strike,
                                                                                        0.015,
                                                                                        0.06,
                                                                                        dte / 365,
                                                                                        vol
                                                                                )
                                                                        niftymd$putcoverprice[i] =
                                                                                greekssl$value
                                                                }
                                                                niftymd$putexitreason[i] =
                                                                        "STOPLOSS"
                                                                niftymd$underlyingputexitprice[i] = coverprice
                                                        } else{
                                                                #check tp
                                                                tp=TRUE
                                                                if(cdte>0){
                                                                        tp = optionprice *
                                                                                365 / (
                                                                                        futurelow * 0.1 * cdte
                                                                                ) < kTPThreshold / 100
                                                                        
                                                                }
                                                                if(tp){
                                                                        niftymd$putcoverprice[i] = (
                                                                                0.1 * futurelow * kTPThreshold * cdte
                                                                        ) / 36500
                                                                        niftymd$putexitdate[i] =
                                                                                paste(
                                                                                        format(
                                                                                                niftymd$date[j],
                                                                                                format = "%Y-%m-%d"
                                                                                        ),
                                                                                        sep = ""
                                                                                )
                                                                        niftymd$putexitreason[i] =
                                                                                "TAKEPROFIT"
                                                                        niftymd$underlyingputexitprice[i] = futuresettle                                                                        
                                                                
                                                                } else if (niftymd$putexittheta[i] /
                                                                           niftymd$putexitvega[i] > kTimeDecayExitRisk) {
                                                                        niftymd$putcoverprice[i] = datarow$settle[1]
                                                                        niftymd$putexitdate[i] =
                                                                                paste(
                                                                                        format(
                                                                                                niftymd$date[j],
                                                                                                format = "%Y-%m-%d"
                                                                                        ),
                                                                                        sep = ""
                                                                                )
                                                                        niftymd$callexitreason[i] =
                                                                                "RISKEXIT"
                                                                        niftymd$underlyingputexitprice[i] = futuresettle
                                                                }
                                                                
                                                        }
                                                }
                                                
                                        }
                                        
                                }
                        }
                }
        }
}



trades = data.frame(
        symbol = character(),
        trade = character(),
        entrytime = as.POSIXct(character()),
        entryprice = numeric(),
        exittime = as.POSIXct(character()),
        exitprice = numeric(),
        percentprofit = numeric(),
        bars = numeric(),
        brokerage = numeric(),
        netpercentprofit = numeric(),
        absolutepnl = numeric(),
        size = numeric(),
        exitreason = character(),
        stringsAsFactors = FALSE
)

niftymd$callentrypercent = niftymd$callprice * 36500 / (niftymd$futuresettle *
                                                                niftymd$cdte * 0.1)
niftymd$putentrypercent = niftymd$putprice * 36500 / (niftymd$futuresettle *
                                                              niftymd$cdte * 0.1)
startindex = head(which(niftymd$date >= kBackTestStartDate), 1)
subset <- niftymd[startindex:nrow(niftymd), ]
longpos = numeric(nrow(subset))
shortpos = numeric(nrow(subset))
exchangemargin=numeric(nrow(subset))
for (i in 1:nrow(subset)) {
        lastrow=FALSE
        if(i==nrow(subset)){
                lastrow=TRUE
        }
        futuresettle=subset[i,'futuresettle']
        expiry = strftime(subset[i, c("contractexpiry")], format = "%Y%m%d")
        right = ifelse(subset[i, c("inshorttrade")] == 1, "CALL", "PUT")
        strike = ifelse(subset[i, c("inshorttrade")] == 1, subset[i, c("callstrike")], subset[i, c("putstrike")])
        symbol = paste("NSENIFTY_OPT", expiry, right, strike, sep = "_")
        entryprice = subset[i, c("callprice")]
        exitprice = subset[i, c("callcoverprice")]
        if(entryprice>0){
                entrytime = as.POSIXct(format(subset[i, c("date")]), tz = kTimeZone)
                exittime=subset[i, c("callexitdate")]
                exittime = as.POSIXct(strptime(exittime,format="%Y-%m-%d",tz=kTimeZone))
                if (is.na(exittime)) {
                        potentialexittime = as.POSIXct(strptime(subset[i, c("contractexpiry")], format =
                                                                        "%Y-%m-%d"), tz = kTimeZone)
                        if (potentialexittime <= as.POSIXct(format(min(Sys.Date(),
                                                                       as.Date(kBackTestEndDate, tz = kTimeZone))),tz=kTimeZone)) {
                                exittime = potentialexittime
                        }
                        if (!is.na(exittime) &&
                            exittime == as.POSIXct("2014-04-24", tz = kTimeZone)) {
                                exittime = as.POSIXct("2014-04-23", tz =kTimeZone)
                        }
                }
                percentprofit = (entryprice - exitprice) * 100 / entryprice
                bars = 0
                pnl = (entryprice - exitprice) - (2*kPerContractBrokerage / 75)
                #        if(subset[i,c("inshorttrade")]==1 && shortpos[i]<kMaxShortPosition && (longpos[i]+shortpos[i])<kMaxTotalPosition && subset$callentryvega[i]<kVegaRisk && subset$callentrytheta[i]<kThetaRisk && subset$callentryvol[i]>kEntryVolThreshold/100 && subset$callentrytheta[i]/subset$callentryvega[i]<kTimeDecayRisk){
                if (subset[i, c("inshorttrade")] == 1 &&
                    longpos[i] < kMaxLongPosition &&
                    #shortpos[i] < kMaxShortPosition &&
                    (longpos[i] + shortpos[i]) < kMaxTotalPosition &&
                    subset$callentryvega[i] < kVegaRisk &&
                    subset$callentrytheta[i] < kThetaRisk &&
                    subset$callentryvol[i] > kEntryVolThreshold / 100 &&
                    subset$callentrytheta[i] / subset$callentryvega[i] < kTimeDecayRisk) {
                        if (subset[i, c("callentrypercent")] >= kReturnThresholdFullSize) {
                                size = min(kFullSize,kMaxTotalPosition-(longpos[i]+shortpos[i]))
                                print(paste("CallReturn:", subset[i, c("callentrypercent")], "i:", i, sep =" "))
                        } else if (subset[i, c("callentrypercent")] >= kReturnThresholdHalfSize) {
                                size = min(kHalfSize,kMaxTotalPosition-(longpos[i]+shortpos[i]))
                        } else{
                                size = 0
                        }
                        
                        if (size > 0) {
                                shortpos[i:length(shortpos)] = shortpos[i:length(shortpos)] + size
                                exchangemargin[i:length(exchangemargin)] = exchangemargin[i:length(exchangemargin)] + size*futuresettle*kExchangeMargin*1.1
                                trades = rbind(
                                        trades,
                                        data.frame(
                                                symbol = symbol,
                                                trade = "SHORT",
                                                entrytime = entrytime,
                                                entryprice = entryprice,
                                                exittime = exittime,
                                                exitprice = exitprice,
                                                percentprofit = percentprofit,
                                                bars = bars,
                                                brokerage = 2*kPerContractBrokerage / ((
                                                        entryprice + exitprice
                                                ) * 75),
                                                netpercentprofit =
                                                        (percentprofit),
                                                absolutepnl = pnl *
                                                        size,
                                                size = size,
                                                exitreason = subset$callexitreason[i],
                                                stringsAsFactors = FALSE
                                        )
                                )
                                if (!is.na(exittime)) {
                                        exitindex = which(
                                                as.POSIXct(format(subset$date), tz = kTimeZone) == exittime
                                        )
                                        if (length(exitindex) == 1) {
                                                shortpos[exitindex:length(shortpos)] = shortpos[exitindex:length(shortpos)] -
                                                        size
                                                exchangemargin[exitindex:length(exchangemargin)] = exchangemargin[exitindex:length(exchangemargin)] -
                                                        size*futuresettle*kExchangeMargin*1.1
                                        }
                                        
                                }
                                
                        }
                        
                }        
        }
        
        expiry = strftime(subset[i, c("contractexpiry")], format = "%Y%m%d")
        right = ifelse(subset[i, c("inlongtrade")] == 1, "PUT", "CALL")
        strike = ifelse(subset[i, c("inlongtrade")] == 1, subset[i, c("putstrike")], subset[i, c("callstrike")])
        symbol = paste("NSENIFTY_OPT", expiry, right, strike, sep = "_")
        entryprice = subset[i, c("putprice")]
        exitprice = subset[i, c("putcoverprice")]
        if(entryprice>0){
                entrytime = as.POSIXct(format(subset[i, c("date")]), tz = kTimeZone)
                exittime=subset[i, c("putexitdate")]
                exittime = as.POSIXct(strptime(exittime,format="%Y-%m-%d",tz=kTimeZone))
                if (is.na(exittime)) {
                        potentialexittime = as.POSIXct(strptime(subset[i, c("contractexpiry")], format =
                                                                        "%Y-%m-%d"), tz = kTimeZone)
                        if (potentialexittime <= as.POSIXct(format(min(Sys.Date(),
                                                                       as.Date(kBackTestEndDate, tz = kTimeZone))),tz=kTimeZone)) {
                                exittime = potentialexittime
                        }
                        if (!is.na(exittime) &&
                            exittime == as.POSIXct("2014-04-24", tz = kTimeZone)) {
                                exittime = as.POSIXct("2014-04-23", tz = kTimeZone)
                        }
                }
                percentprofit = (entryprice - exitprice) * 100 / entryprice
                bars = 0
                pnl = (entryprice - exitprice) - (2*kPerContractBrokerage / 75)
                
                #        if(subset[i,c("inlongtrade")]==1 && longpos[i]<kMaxLongPosition  && (longpos[i]+shortpos[i])<kMaxTotalPosition && subset$putentryvega[i]<kVegaRisk && subset$putentrytheta[i]<kThetaRisk && subset$putentryvol[i]>kEntryVolThreshold/100 && subset$putentrytheta[i]/subset$putentryvega[i]<kTimeDecayRisk){
                if (subset[i, c("inlongtrade")] == 1 &&
                    shortpos[i] < kMaxShortPosition  &&
                    #longpos[i] < kMaxLongPosition  &&
                    (longpos[i] + shortpos[i]) < kMaxTotalPosition &&
                    subset$putentryvega[i] < kVegaRisk &&
                    subset$putentrytheta[i] < kThetaRisk &&
                    subset$putentryvol[i] > kEntryVolThreshold / 100 &&
                    subset$putentrytheta[i] / subset$putentryvega[i] < kTimeDecayRisk) {
                        if (subset[i, c("putentrypercent")] >= kReturnThresholdFullSize) {
                                size = min(kFullSize,kMaxTotalPosition-(longpos[i]+shortpos[i]))
                                print(paste("PutReturn:", subset[i, c("putentrypercent")], "i:", i, sep =
                                                    ","))
                                
                        } else if (subset[i, c("putentrypercent")] >= kReturnThresholdHalfSize) {
                                size = min(kHalfSize,kMaxTotalPosition-(longpos[i]+shortpos[i]))
                        } else{
                                size = 0
                        }
                        if (size > 0) {
                                longpos[i:length(longpos)] = longpos[i:length(longpos)] + size
                                exchangemargin[i:length(exchangemargin)] = exchangemargin[i:length(exchangemargin)] + size*futuresettle*kExchangeMargin*1.1
                                trades = rbind(
                                        trades,
                                        data.frame(
                                                symbol = symbol,
                                                trade = "SHORT",
                                                entrytime = entrytime,
                                                entryprice = entryprice,
                                                exittime = exittime,
                                                exitprice = exitprice,
                                                percentprofit = percentprofit,
                                                bars = bars,
                                                brokerage = 2*kPerContractBrokerage / ((
                                                        entryprice + exitprice
                                                ) * 75),
                                                netpercentprofit =
                                                        percentprofit,
                                                absolutepnl = pnl *
                                                        size,
                                                size = size,
                                                exitreason = subset$putexitreason[i],
                                                stringsAsFactors = FALSE
                                        )
                                )
                                if (!is.na(exittime)) {
                                        exitindex = which(
                                                as.POSIXct(format(subset$date), tz = kTimeZone) == exittime
                                        )
                                        if (length(exitindex) == 1) {
                                                longpos[exitindex:length(longpos)] = longpos[exitindex:length(longpos)] -
                                                        size
                                                exchangemargin[exitindex:length(exchangemargin)] = exchangemargin[exitindex:length(exchangemargin)] -
                                                        size*futuresettle*kExchangeMargin*1.1
                                        }
                                }
                                
                                
                        }
                        
                }        
        }
        
        if(lastrow){
                if(subset[i, c("inlongtrade")] == 1){
                        levellog(logger,
                                 "INFO",
                                 paste(symbol,longpos[i],shortpos[i],subset$settle[i],subset$putentryvega[i],
                                       subset$putentrytheta[i],subset$putentryvol[i],
                                       subset$putentrytheta[i] / subset$putentryvega[i],
                                       subset$putentrypercent[i],sep = ":"))
                }else{
                        levellog(logger,
                                 "INFO",
                                 paste(symbol,longpos[i],shortpos[i],subset$settle[i],subset$callentryvega[i],
                                       subset$callentrytheta[i],subset$callentryvol[i],
                                       subset$callentrytheta[i] / subset$callentryvega[i],
                                       subset$callentrypercent[i],sep = ":"))
                        
                }
        }
}

if(nrow(trades)>0){
        BizDayBacktestEnd=adjust("India",min(Sys.Date(),
                                             as.Date(kBackTestEndDate, tz = kTimeZone)),bdc=2)
        for (t in 1:nrow(trades)) {
                expirydate = as.Date(unlist(strsplit(trades$symbol[t], "_"))[3], "%Y%m%d", tz =
                                             kTimeZone)
                if (is.na(trades$exitreason[t]) &&
                    (
                            (expirydate > min(Sys.Date(),
                                              as.Date(kBackTestEndDate, tz = kTimeZone)))
                    )){
                        symbolsvector=unlist(strsplit(trades$symbol[t],"_"))
                        load(paste(kFNODataFolder,symbolsvector[3],"/", trades$symbol[t], ".Rdata", sep = ""))
                        index=which(as.Date(md$date,tz=kTimeZone)==BizDayBacktestEnd)
                        if(length(index)==1){
                                trades$exitprice[t] = md$settle[index]
                        }else{
                                trades$exitprice[t] = tail(md$settle,1)
                        }
                        trades$absolutepnl[t] = (trades$entryprice[t] - trades$exitprice[t] -
                                                         (kPerContractBrokerage / 75)) * trades$size[t]
                        
                }
        }
        
}

entrysize = 0
exitsize = 0
if (length(which(as.Date(trades$entrytime,tz=kTimeZone) == Sys.Date())) == 1) {
        entrysize = trades[as.Date(trades$entrytime,tz=kTimeZone) == Sys.Date(), c("size")][1]
}
if (length(which(as.Date(trades$exittime,tz=kTimeZone) == Sys.Date())) >= 1) {
        exittime=which(as.Date(trades$exittime,tz=kTimeZone) == Sys.Date())
        exitsize = sum(trades[exittime, c("size")])
}

#Exit First, then enter
#Write Exit to Redis

if (exitsize > 0 & kWriteToRedis) {
        redisConnect()
        redisSelect(args[3])
        out <- trades[which(as.Date(trades$exittime,tz=kTimeZone) == Sys.Date()),]
        for (o in 1:nrow(out)) {
                startingposition = abs(GetCurrentPosition(out[o, "symbol"], trades)) + out[o, "size"]
                redisString = paste(out[o, "symbol"],
                                    out[o, "size"],
                                    "COVER",
                                    0,
                                    abs(startingposition),
                                    sep = ":")
                redisRPush(paste("trades", args[2], sep = ":"),
                           charToRaw(redisString))
                levellog(logger,
                         "INFO",
                         paste(args[2], redisString, sep = ":"))
                
        }
        redisClose()
}

if (entrysize > 0 & kWriteToRedis) {
        redisConnect()
        redisSelect(args[3])
        out <- trades[which(as.Date(trades$entrytime,tz=kTimeZone) == Sys.Date()),]
        for (o in 1:nrow(out)) {
                startingposition = abs(GetCurrentPosition(out[o, "symbol"], trades)) - out[o, "size"]
                redisString = paste(out[o, "symbol"],
                                    out[o, "size"],
                                    "SHORT",
                                    0,
                                    abs(startingposition),
                                    sep = ":")
                redisRPush(paste("trades", args[2], sep = ":"),
                           charToRaw(redisString))
                levellog(logger,
                         "INFO",
                         paste(args[2], redisString, sep = ":"))
                
        }
        redisClose()
}
niftymd<-cbind(niftymd,longpos,shortpos)
save(niftymd,file="niftymd.R")
save(trades,file="trades.R")

#tempdates<-seq(from=as.Date(kBackTestStartDate,tz=kTimeZone),to=as.Date(min(Sys.Date(),as.Date(kBackTestEndDate,tz=kTimeZone))),by=1)
#bizdays<-isBusinessDay("India",tempdates)
pnl<-data.frame(bizdays=as.Date(subset$date,tz=kTimeZone),realized=0,unrealized=0,brokerage=0)
cumpnl<-CalculateDailyPNL(trades,pnl,kFNODataFolder,kPerContractBrokerage/75,deriv=TRUE)

DailyPNL <-
        (cumpnl$realized + cumpnl$unrealized-cumpnl$brokerage) - Ref(cumpnl$realized + cumpnl$unrealized-cumpnl$brokerage, -1)
DailyPNL <- ifelse(is.na(DailyPNL), 0, DailyPNL)
DailyReturn <-
        ifelse(exchangemargin == 0, 0, DailyPNL / exchangemargin)
DailyReturn<-DailyPNL/(mean(subset$futuresettle)*kExchangeMargin*1.1)
df <- data.frame(time = as.Date(subset$date,tz=kTimeZone), return = DailyReturn)
df <- read.zoo(df)
sharpe <-
        SharpeRatio((df[df != 0][, 1, drop = FALSE]), Rf = .07 / 365, FUN = "StdDev") *
        sqrt(252)
print(paste("sharpe:", sharpe, sep = ""))


holdingyears=as.numeric(subset$date[nrow(subset)]-subset$date[1])/365
return<-sum(trades$absolutepnl) * 100 / (kMaxTotalPosition * mean(subset$futuresettle) * kExchangeMargin)
print(paste("Annual Return:",return/holdingyears))

print(paste("winratio:", sum(trades$absolutepnl > 0) / nrow(trades), sep =
                    ""))
print(paste("Max Position:", max(longpos + shortpos), sep = ""))
print(paste(
        "Average Utilization:",
        mean(shortpos + longpos) / max(shortpos + longpos),
        sep = ""
))

if(nrow(cumpnl)>1){
        cumpnl$group <- strftime(cumpnl$bizdays, "%Y%m")
        cumpnl$dailypnl<-DailyPNL
        dd.agg <- aggregate(dailypnl ~ group, cumpnl, FUN = sum)
        print(paste("Average Loss in Losing Month:",mean(dd.agg[which(dd.agg$dailypnl<0),'dailypnl']),sep=""))
        print(paste("Percentage Losing Months:",sum(dd.agg$dailypnl<0)/nrow(dd.agg)))
}

tail(niftymd[, c("date",
                 "callstrike",
                 "putstrike",
                 "callentrypercent",
                 "putentrypercent")])
