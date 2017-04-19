library("RPostgreSQL")
library("plyr")

dwConnect<-function(){
  library("RPostgreSQL")
  dbHost<-"52.64.224.248" # public read replica, only accessible outside DW data center
  dbPort<-8000
  if (!is.na(Sys.getenv("IS_PRODUCTION", NA))) {
    # master, only accessible inside DW data center
    dbHost<-"dw-staging.cjza6pmqs6im.ap-southeast-2.rds.amazonaws.com"
    dbPort<-5432
  }

  pg <- DBI::dbDriver("PostgreSQL")
  con<-DBI::dbConnect(pg, user="betia_staging", password="poT5oT4Ayct0Eef5vin2Arb7owG3oo",
                 host=dbHost, port=dbPort, dbname="dw_staging")
  return(con)
}

#' Allocates market type Tote/Fixed
#'
#' @keywords intraday
#' @export
#' @examples
#'
typeAlloc<-function(market){
  if(is.na(market)) return(NA)
  else if(grepl('tab',market)) return('WIN')
  else return('WIN FX')
}

getHostName<-function(venue_id){
  x<-DBI::dbGetQuery(con, paste("select venues.host_market as host
                       from venues
                                where venues.id = ",venue_id,";",sep=""))[1,1]
  return(x)
}

marketQuery<-function(market,type,ind=0){
  if(is.na(market)) res<-paste('(select market_json::json->\'prices\'->event_competitor_race_data.number-1 from markets where markets.provider = \'',market,'\' and market_name = \'WIN\' and markets.meeting_id = meetings.id and markets.event_number = events.number limit 1) as ',market,sep="")
  else if(ind==1) res<-paste('(select market_json::json->\'prices\'->event_competitor_race_data.number-1 from markets where markets.provider = \'',market,'\' and market_name = \'WIN\' and markets.meeting_id = meetings.id and markets.event_number = events.number limit 1) as host',sep="")
  else if(market=='citibet') res<-paste('(select market_json::json->\'available\'->event_competitor_race_data.number::text->0->>2 from markets where markets.provider = \'',market,'\' and market_name = \'EATW\' and markets.meeting_id = meetings.id and markets.event_number = events.number limit 1) as ',market,sep="")
  else if(grepl('betia',market)) res<-paste('coalesce((select analysis_json::json->\'data\'->event_competitor_race_data.number::text->\'win_price\' from market_analyses where market_name = \'',type,'\' and market_analyses.meeting_id = meetings.id and market_analyses.event_number = events.number limit 1),\'{}\') as ',market,sep="")
  else if(type=='BACK' | type=='LAY') res<-paste('(select market_json::json->\'volume_prices\'->event_competitor_race_data.number-1->0->\'price\' from markets where market_name = \'',type,'\' and markets.meeting_id = meetings.id and markets.event_number = events.number limit 1) as ',market,sep="")
  else if(market=='betfair') res<-paste('(select market_json::json->\'prices\'->event_competitor_race_data.number-1 from markets where market_name = \'',type,'\' and markets.meeting_id = meetings.id and markets.event_number = events.number limit 1) as ',market,sep="")
  else res<-paste('(select market_json::json->\'prices\'->event_competitor_race_data.number-1 from markets where markets.provider = \'',market,'\' and market_name = \'',type,'\' and markets.meeting_id = meetings.id and markets.event_number = events.number limit 1) as ',market,sep="")
  return(res)
}

betiaFix<-function(type){
  if(is.na(type)) return(NA)
  else if(type=='A1') return('analyst_1')
  else if(type=='A2') return('analyst_2')
  else return(type)
}

#' Fetches data for Market Diagnostics
#'
#' @keywords intraday
#' @export
#' @examples
#' fetchData(params) where params<-c(market_name,market_name,market_name,dfrom,dto,course,venue_type)
fetchData<-function(params){
  library("RPostgreSQL")
  markets<-params[1:3]
  dfrom<-as.Date(params[4])
  dto<-as.Date(params[5])
  venue_id<-as.numeric(params[6])

  con<-dwConnect()

  host<-DBI::dbGetQuery(con, paste("select venues.host_market as host from venues where venues.id = ",venue_id,";",sep=""))[1,1]

  m.1<-unlist(strsplit(params[1],"/"))[1]
  m.name.1<-unlist(strsplit(params[1],"/"))[2]

  m.name.1<-betiaFix(m.name.1)

  m.2<-unlist(strsplit(params[2],"/"))[1]
  m.name.2<-unlist(strsplit(params[2],"/"))[2]

  m.name.2<-betiaFix(m.name.2)

  m.3<-unlist(strsplit(params[3],"/"))[1]
  m.name.3<-unlist(strsplit(params[3],"/"))[2]

  m.name.3<-betiaFix(m.name.3)

  x<-list()
  t1<-marketQuery(m.1,m.name.1)
  t2<-marketQuery(m.2,m.name.2)
  t3<-marketQuery(m.3,m.name.3)
  host<-marketQuery(host,'WIN',1)

  dbQuery<-paste("Select meetings.id as meeting_id, events.id as event_id, event_competitors.id as event_competitor_id, competitors.id as competitor_id, trainers.id as trainer_id, venues.name as venue_name, meeting_date, countries.name as country_name, events.number as event_number, competitors.name as competitor_name, trainers.name as trainer_name,event_competitor_race_data.program_number ,event_competitor_race_data.barrier, event_competitor_race_data.finish_position, event_race_data.distance, event_race_data.race_class,
                       venue_types.name as venue_type_name, event_competitor_race_data.scratched as is_scratched,

                       (SELECT event_status_types.name FROM \"event_statuses\" inner join event_status_types on event_status_types.id = event_statuses.event_status_type_id WHERE \"event_statuses\".\"event_id\" = events.id  ORDER BY timestamp DESC LIMIT 1) as event_status,
                       ",t1,",
                       ",t2,",
                       ",t3,",
                       ",host,"

                       from meetings
                       left outer join venues on venues.id = meetings.venue_id
                       left outer join countries on countries.id = venues.country_id
                       left outer join venue_types on venue_types.id = venues.venue_type_id
                       left outer join events on events.meeting_id = meetings.id
                       left outer join event_competitors on event_competitors.event_id = events.id
                       left outer join competitors on event_competitors.competitor_id = competitors.id
                       left outer join event_competitor_race_data on event_competitor_race_data.id = event_competitors.event_competitor_race_datum_id
                       left outer join trainers on trainers.id = event_competitor_race_data.trainer_id
                       left outer join event_race_data on event_race_data.id = events.event_race_datum_id
                       WHERE meeting_date >= \'",dfrom,"\' and meeting_date <= \'",dto,"\' and venues.id = ",venue_id," and event_competitor_race_data.scratched = FALSE;",sep="")
  x<-DBI::dbGetQuery(con,dbQuery)
  x<-x[x$event_status=='CLOSED' | x$event_status=='FINAL',]
  return(x)
}

betPay<-function(stake,result,odds){
  if(is.na(stake) | is.na(result) | is.na(odds)) return(NA)
  else if(result==1) return(stake*(odds-1))
  else return(-stake)
}

citibetSummarize<-function(data,market='host'){
  lows<-c(76,76,80,84,88,92,96)
  ups<-c(76,80,84,88,92,96,100)
  lows<-rev(lows)
  ups<-rev(ups)


  res<-as.data.frame(matrix(NA,length(lows),3))
  colnames(res)<-c('lower_ctb','upper_ctb','roi')



  res$lower_ctb<-lows
  res$upper_ctb<-ups
  data[,c(market)]<-as.numeric(data[,c(market)])
  mkt<-as.numeric(data[,c(market)])
  data$citibet_discount<-as.numeric(data$citibet_discount)
  data$citibet_stake<-100/(data$host-1)
  data$citibet_profit<-mapply(betPay,data$citibet_stake,data$finish_position,data$host)

  for(i in 1:length(lows)){
    up_ctb<-res$upper_ctb[i]
    low_ctb<-res$lower_ctb[i]


    if(up_ctb==76) filter_ctb<-data$citibet_discount==up_ctb
    else if(up_ctb==80) filter_ctb<-data$citibet_discount>low_ctb & data$citibet_discount<up_ctb & is.finite(data$citibet_discount)
    else filter_ctb<-data$citibet_discount>=low_ctb & data$citibet_discount<up_ctb & is.finite(data$citibet_discount)

    aa<-data[filter_ctb,]
    n<-nrow(aa)
    bb<-aa[aa$finish_position==1 & is.finite(aa$finish_position),]
    w<-nrow(bb)
    e<-(1/mean(aa$citibet_price,na.rm=T))
    a<-w/n
    roi<-res$roi[i]<-(sum(aa$citibet_profit,na.rm=T)/sum(aa$citibet_stake,na.rm=T))+1
  }
  return(res)
}

citibetTable<-function(data,market='host'){
  lows<-c(76,76,80,84,88,92,96)
  ups<-c(76,80,84,88,92,96,100)
  lows<-rev(lows)
  ups<-rev(ups)


  o_low<-c(0.0001,0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5)
  o_up<-c(0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5,1)
  o_low<-rev(o_low)
  o_up<-rev(o_up)

  res<-as.data.frame(matrix(NA,length(o_low)*length(lows),10))
  colnames(res)<-c('lower_odds','upper_odds','lower_ctb','upper_ctb','act','exp','roi','n','w','chi')

  u_cols<-rep(ups,length(o_low))
  l_cols<-rep(lows,length(o_low))

  res$lower_odds<-rev(sort(rep(o_low,length(lows))))
  res$upper_odds<-rev(sort(rep(o_up,length(lows))))
  res$lower_ctb<-l_cols
  res$upper_ctb<-u_cols
  data[,c(market)]<-as.numeric(data[,c(market)])
  mkt<-as.numeric(data[,c(market)])
  data$citibet_discount<-as.numeric(data$citibet_discount)
  data$citibet_stake<-100/(data$host-1)
  data$citibet_profit<-mapply(betPay,data$citibet_stake,data$finish_position,data$host)
  for(i in 1:nrow(res)){
    up_ctb<-res$upper_ctb[i]
    low_ctb<-res$lower_ctb[i]

    up_odds<-(1/res$lower_odds[i])
    low_odds<-(1/res$upper_odds[i])


    filter_odds<-data[,c(market)]>=low_odds & data[,c(market)]<up_odds & is.finite(data[,c(market)])

    if(up_ctb==76) filter_ctb<-data$citibet_discount==up_ctb
    else if(up_ctb==80) filter_ctb<-data$citibet_discount>low_ctb & data$citibet_discount<up_ctb & is.finite(data$citibet_discount)
    else filter_ctb<-data$citibet_discount>=low_ctb & data$citibet_discount<up_ctb & is.finite(data$citibet_discount)

    aa<-data[filter_odds & filter_ctb,]
    n<-res$n[i]<-nrow(aa)
    bb<-aa[aa$finish_position==1 & is.finite(aa$finish_position),]
    w<-res$w[i]<-nrow(bb)
    e<-res$exp[i]<-(1/mean(aa$citibet_price,na.rm=T))
    a<-res$act[i]<-w/n
    roi<-res$roi[i]<-(sum(aa$citibet_profit,na.rm=T)/sum(aa$citibet_stake,na.rm=T))+1
    res$chi[i]<-((w-(e)*n)^2)/(e*n)

    flush.console()
  }
  res$lower_odds<-round(res$lower_odds,2)
  res<-plyr::dlply(res,~lower_odds)
  res<-rev(res)
  return(res)
}

#' Produces chi-square table
#'
#' @keywords chi-square
#' @export
#' @examples
#' chiSquareClassic(data,'tab_vic')
chiSquareClassic<-function(data,market){
  lows<-c(0.0000001,0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5)
  ups<-c(0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5,1)

  res<-as.data.frame(matrix(NA,length(lows),7))
  colnames(res)<-c('Lower','Upper','N','W','Act','Exp','Chi')


  data[,c(market)]<-as.numeric(data[,c(market)])

  if(market=='citibet') market<-'citibet_price'
  mkt<-as.numeric(data[,c(market)])


  res$Lower<-lows
  res$Upper<-ups
  for(i in 1:nrow(res)){
    u<-(1/res$Upper[i])
    l<-(1/res$Lower[i])
    filter<-mkt>=u & mkt<l & is.finite(mkt)
    aa<-data[filter,]
    bb<-aa[aa$finish_position==1 & is.finite(aa$finish_position),]
    total<-res$N[i]<-nrow(aa)
    w<-res$W[i]<-nrow(bb)
    act<-res$Act[i]<-w/total
    exp<-res$Exp[i]<-(1/mean(aa[,c(market)],na.rm=T))
    res$Chi[i]<-((w-(exp)*total)^2)/(exp*total)
    flush.console()
  }
  res<-res[order(-res$Lower),]
  res$Lower<-round(res$Lower,2)
  res_chi<-sum(res$Chi,na.rm=T)
  colnames(res)<-tolower(colnames(res))
  return(res)
}

tradeBackTable<-function(data,market_1,market_2){
  lows<-c(0.0001,0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5)
  ups<-c(0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5,1)

  res<-as.data.frame(matrix(NA,length(lows),7))
  colnames(res)<-c('Lower','Upper','n','w','outlay','return','roi')


  mkt<-data[,c(market_1)]<-as.numeric(data[,c(market_1)])
  data[,c(market_2)]<-as.numeric(data[,c(market_2)])

  if(market_1=='citibet') market_1<-'citibet_price'
  else if(market_2=='citibet') market_2<-'citibet_price'

  data$market_1_exp<-data[,c(market_2)]/data[,c(market_1)]
  data$market_1_stake<-100/(data[,c(market_1)]-1)
  data$market_1_profit<-mapply(betPay,data$market_1_stake,data$finish_position,data[,c(market_2)])

  res$Lower<-lows
  res$Upper<-ups

  for(i in 1:nrow(res)){
    u<-(1/res$Upper[i])
    l<-(1/res$Lower[i])
    filter<-mkt>=u & mkt<l & is.finite(mkt) & is.finite(data$market_1_exp) & data$market_1_exp>=1.1

    aa<-data[filter,]
    bb<-aa[aa$finish_position==1 & is.finite(aa$finish_position),]

    n<-res$n[i]<-nrow(aa)
    w<-res$w[i]<-nrow(bb)
    outlay<-res$outlay[i]<-sum(aa$market_1_stake,na.rm=T)
    return<-res$return[i]<-sum(aa$market_1_profit,na.rm=T)+outlay
    roi<-res$roi[i]<-return/outlay
    flush.console
  }
  res<-res[order(-res$Lower),]
  colnames(res)<-tolower(colnames(res))
  return(res)
}



marketCorrelation<-function(data,market,type){
  if(grepl("/",data$meeting_date[1])) data$meeting_date<-as.Date(data$meeting_date,"%d/%m/%Y")

  if(market=='citibet') market<-'citibet_price'
  data$market_rank<-ave(as.numeric(data[,c(market)]),interaction(data$event_id),FUN=rank)
  data$market_rank[is.na(data[,c(market)])]<-NA
  data<-data[is.finite(data$market_rank) & is.finite(data$finish_position),]

  if(type=='pearson') res<-cor(as.numeric(data$finish_position),as.numeric(data[,c(market)]),method='pearson')
  else res<-cor(as.numeric(data$market_rank),as.numeric(data$finish_position),method='spearman')

  return(res)
}

chiSquareTotal<-function(data){
  res<-sum(data$chi,na.rm=T)
  return(res)
}

chiCollater<-function(data,params){
  markets<-params[1:3]
  markets[1]<-sub('/.*', '', markets[1])
  markets[2]<-sub('/.*', '', markets[2])
  markets[3]<-sub('/.*', '', markets[3])
  markets<-markets[!is.na(markets)]
  total<-length(markets)
  ind<-params[length(params)]
  x<-list()

  if('citibet' %in% markets) data$citibet_discount<-data$citibet
  if('citibet' %in% markets) data$citibet_price<-(as.numeric(data$host))/(as.numeric(data$citibet)/100)

  for(i in 1:total){
    res<-x$tables[[i]]<-chiSquareClassic(data,markets[i])
    x$summary$chi[[i]]<-chiSquareTotal(res)
    x$summary$rsq[[i]]<-marketCorrelation(data,markets[i],'pearson')
    x$summary$spearman[[i]]<-marketCorrelation(data,markets[i],'spearman')

    if(ind==1 & markets[i]=='citibet') x$aex$summary[[i]]<-citibetSummarize(data)
    if(ind==1 & markets[i]=='citibet') aex<-x$aex$data[[i]]<-citibetTable(data)


    if(length(markets)<2) next
    if(i==1) x$trade_back[[i]]<-tradeBackTable(data,markets[i],markets[2])
    else if(i==2) x$trade_back[[i]]<-tradeBackTable(data,markets[i],markets[1])
    else if(i==3) next

    flush.console()
  }
  return(x)
}

masterDiagnostics<-function(params){
  data<-fetchData(params)
  res<-chiCollater(data,params)
  return(res)
}
