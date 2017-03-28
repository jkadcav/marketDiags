library(RPostgreSQL)

dwConnect<-function(){
  dbHost<-"52.64.224.248" # public read replica, only accessible outside DW data center
  dbPort<-8000
  if (!is.na(Sys.getenv("IS_PRODUCTION", NA))) {
    # master, only accessible inside DW data center
    dbHost<-"dw-staging.cjza6pmqs6im.ap-southeast-2.rds.amazonaws.com"
    dbPort<-5432
  }

  pg <- dbDriver("PostgreSQL")
  con<-dbConnect(pg, user="betia_staging", password="poT5oT4Ayct0Eef5vin2Arb7owG3oo",
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

#' Fetches data for Market Diagnostics
#'
#' @keywords intraday
#' @export
#' @examples
#' fetchData(params) where params<-c(market_name,market_name,market_name,dfrom,dto,course,venue_type)
fetchData<-function(params){
  con<-dwConnect()
  markets<-params[1:3]
  print(markets)
  dfrom<-as.Date(params[4])
  dto<-as.Date(params[5])
  venue<-params[6]
  print(dfrom)
  print(venue)
  x<-list()

  t1<-typeAlloc(markets[1])
  t2<-typeAlloc(markets[2])
  t3<-typeAlloc(markets[3])

  x<-dbGetQuery(con,paste("Select meetings.id as meeting_id, events.id as event_id, event_competitors.id as event_competitor_id, competitors.id as competitor_id, trainers.id as trainer_id, venues.name as venue_name, meeting_date, countries.name as country_name, events.number as event_number, competitors.name as competitor_name, trainers.name as trainer_name,event_competitor_race_data.program_number ,event_competitor_race_data.barrier, event_competitor_race_data.finish_position, event_race_data.distance, event_race_data.race_class,
                          venue_types.name as venue_type_name, event_competitor_race_data.scratched as is_scratched,
                          (select market_json::json->'prices'->event_competitor_race_data.number-1 from markets where markets.provider = \'",markets[1],"\' and market_name = \'",t1,"\' and markets.meeting_id = meetings.id and markets.event_number = events.number limit 1) as ",markets[1],",
                          (select market_json::json->'prices'->event_competitor_race_data.number-1 from markets where markets.provider = \'",markets[2],"\' and market_name = \'",t2,"\' and markets.meeting_id = meetings.id and markets.event_number = events.number limit 1) as ",markets[2],",
                          (select market_json::json->'prices'->event_competitor_race_data.number-1 from markets where markets.provider = \'",markets[3],"\' and market_name = \'",t3,"\' and markets.meeting_id = meetings.id and markets.event_number = events.number limit 1) as ",markets[3],"
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
                          WHERE venue_types.name = 'THOROUGHBRED' and countries.name = 'Australia' and meeting_date >= \'",dfrom,"\' and meeting_date <= \'",dto,"\' and venues.name = \'",venue,"\' and event_competitor_race_data.scratched = FALSE;",sep=""))
  return(x)
}


#' Produces chi-square table
#'
#' @keywords chi-square
#' @export
#' @examples
#' chiSquareClassic(data,'tab_vic')
chiSquareClassic<-function(data,market){
  lows<-c(0.0000001,0.1,0.2,0.3,0.4,0.5)
  ups<-c(0.1,0.2,0.3,0.4,0.5,1)

  res<-as.data.frame(matrix(NA,length(lows),6))
  colnames(res)<-c('Lower','Upper','N','Act','Exp','Chi')

  data[,c(market)]<-as.numeric(data[,c(market)])
  mkt<-as.numeric(data[,c(market)])
  res$Lower<-lows
  res$Upper<-ups
  for(i in 1:nrow(res)){
    u<-(1/res$Upper[i])
    l<-(1/res$Lower[i])
    filter<-mkt>=u & mkt<l & is.finite(mkt)
    aa<-data[filter,]
    bb<-aa[aa$finish_position==1,]
    res$N[i]<-nrow(aa)
    act<-res$Act[i]<-nrow(bb)
    #market<-
    exp<-res$Exp[i]<-(1/mean(aa[,c(market)],na.rm=T))*nrow(aa)
    res$Chi[i]<-((act-exp)^2)/exp
    flush.console()
  }
  res$Lower<-round(res$Lower,1)
  res_chi<-sum(res$Chi,na.rm=T)
  return(res)
}



masterDiagnostics<-function(params){
  data<-fetchData(params)
  res<-chiSquareClassic(data,params[1])
  return(res)
}
