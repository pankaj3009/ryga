createPNLSummary<-function(redisdb,pattern,start,end,mdpath){
        redisConnect()
        redisSelect(redisdb)
        rediskeys=redisKeys()
        actualtrades <- data.frame(symbol=character(),trade=character(),entrysize=as.numeric(),entrytime=as.POSIXct(character()),
                             entryprice=numeric(),exittime=as.POSIXct(character()),exitprice=as.numeric(),
                             percentprofit=as.numeric(),bars=as.numeric(),brokerage=as.numeric(),
                             netpercentprofit=as.numeric(),netprofit=as.numeric(),key=character(),stringsAsFactors = FALSE
        )
        periodstartdate=as.Date(start,tz="Asia/Kolkata")
        periodenddate=as.Date(end,tz="Asia/Kolkata")
        
        rediskeysShortList<-as.character()
        for(i in 1:length(rediskeys)){
                l=length(grep(pattern,rediskeys[i]))
                if(l>0){
                        if(grepl("opentrades",rediskeys[i])||grepl("closedtrades",rediskeys[i])){
                                rediskeysShortList<-rbind(rediskeysShortList,rediskeys[i])
                                }
                }
        }
        rediskeysShortList<-sort(rediskeysShortList)
        # loop through keys and generate pnl
        for(i in 1:length(rediskeysShortList)){
                data<-unlist(redisHGetAll(rediskeysShortList[i]))
                exitprice=0
                entrydate=data["entrytime"]
                exitdate=data["exittime"]
                entrydate=as.Date(entrydate,tz="Asia/Kolkata")
                exitdate=as.Date(ifelse(is.na(exitdate),Sys.Date(),as.Date(exitdate,tz="Asia/Kolkata")))
                if(exitdate>=periodstartdate && entrydate<=periodenddate){
                        if(grepl("opentrades",rediskeysShortList[i])){
                                symbol=data["entrysymbol"]
                                load(paste(mdpath,symbol,".Rdata",sep=""))
                                index=which(as.Date(md$date,tz="Asia/Kolkata")==exitdate)
                                if(length(index)==0){
                                        index=nrow(md)
                                }
                                exitprice=md$settle[index]
                        }else{
                                exitprice=as.numeric(data["exitprice"])
                        }
                        percentprofit=(exitprice-as.numeric(data["entryprice"]))/as.numeric(data["entryprice"])
                        percentprofit=ifelse(data["entryside"]=="BUY",percentprofit,-percentprofit)
                        brokerage=as.numeric(data["entrybrokerage"])+as.numeric(data["exitbrokerage"])
                        netpercentprofit=percentprofit-(brokerage/(as.numeric(data["entryprice"])*as.numeric(data["entrysize"])))
                        netprofit=(exitprice-as.numeric(data["entryprice"]))*as.numeric(data["entrysize"])-brokerage
                        netprofit=ifelse(data["entryside"]=="BUY",netprofit,-netprofit)
                        df=data.frame(symbol=data["parentsymbol"],trade=data["entryside"],entrysize=as.numeric(data["entrysize"]),entrytime=data["entrytime"],
                                      entryprice=as.numeric(data["entryprice"]),exittime=data["exittime"],exitprice=exitprice,
                                      percentprofit=percentprofit,bars=0,brokerage=brokerage,
                                      netpercentprofit=netpercentprofit,netprofit=netprofit,key=rediskeysShortList[i],stringsAsFactors = FALSE
                        ) 
                        rownames(df)<-NULL
                        actualtrades=rbind(actualtrades,df)
                        
                }
        }
        #return(actualtrades[with(trades,order(entrytime)),])
        return(actualtrades)
}

#backtest
source("yield01_v1.1.R")
realtrades<-createPNLSummary(0,"swing01","2017-01-01","2017-01-31","/home/psharma/Seafile/rfiles/daily-fno/")
