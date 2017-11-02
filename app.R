#Set Library
library("RecordLinkage")
library("stringdist")
library(RODBC)
library(shiny)


#Set the working Directory
Directory <- "~/Unicorn/RWorkingFiles/PartialMatching"
setwd(Directory)

#Extracting Database List
dbCon <- odbcDriverConnect('driver={SQL Server};server=configuratorsql.traxtech.com;database=CentralConfigRepl;trusted_connection=true')
dblist <- sqlQuery(dbCon,"select wg.sqldns DNS
                   ,EnvName DBName
                   ,wg.generation
                   ,[OwnerKey]
                   ,DatabaseName 
                   from [fps].[WorkGroup] wg (Nolock)
                   inner join [fps].[Environment] e on wg.WorkGroupId=e.WorkGroupId
                   where (not EnvName like '%snap%' 
                   and not EnvName like '%test%' 
                   and not EnvName like '%match%' 
                   and not EnvName like '%visibility%'
                   and not EnvName like '%Migration%'
                   and not EnvName like '%pre-%'
                   and not EnvName like '%mock%'
                   and not EnvName like '%PreProcessor%'
                   and not EnvName like '%demo%')
                   and Purpose = 'Production'
                   order by EnvName")
dblist$OwnerKeyData <- ifelse(dblist$generation=="Chameleon",gsub("0-", "1", dblist$OwnerKey),gsub("0-", "0", dblist$OwnerKey))
dblist <- dblist[order(dblist$DBName),]
odbcClose(dbCon)
lookup <- function(x){
  library(RODBC)
  CustDetail <-dblist[grep(x,dblist$DBName,ignore.case = T),]
  ServerName <- CustDetail$DNS[1]
  DBName <- CustDetail$DatabaseName[1]
  CustKey <- substr(CustDetail$OwnerKey[1],5,8)
  CustData <- CustDetail$OwnerKeyData[1]
  
  dbhandle <- odbcDriverConnect(paste("driver={SQL Server};server=",ServerName,";database=",DBName,";trusted_connection=true",sep=""))
  
  res <- if(CustDetail$DatabaseName=="MultiClient01" | CustDetail$DatabaseName=="MultiClient02")
  {sqlQuery(dbhandle, paste("select f.owner_key as 'owner_key'
                                    ,f.vend_labl
                            ,'' as 'col1'
                            ,'' as 'col2'
                            ,'' as 'col3'
                            ,l.chrg_desc as [LookupDesc]
                            ,'' as 'CanonChargeCode'
                            ,Replace(SYSTEM_USER,'FILEX\\Ruben.Felomino','yourname') as 'UserName'
                            ,CONVERT (DATE, GETDATE()) as 'UpdatedOn'
                            ,max(fb_key) as 'samplefb'
                            ,count(f.fb_key) as 'AffectedFB'
                            from outfrght_bl f (nolock)
                            inner join outfb_ln l (Nolock) on f.fb_id=l.fb_id
                            inner join outfrght_bl_ext fbx with (nolock) on fbx.fb_id = f.fb_id
                            inner join Activity.SubActivityResult r with (nolock) on r.untnid = fbx.fbnid
                            inner join [Activity].[SubActivityDetailHistory] h with (nolock) on r.UntNId = h.UntNId and r.SubActivityId = h.SubActivityId and r.MostRecentExecutionDtmUtc = h.ExecutionDtmUtc  
                            left join CentralConfigRepl.tdrActivity.SubActivity u with (nolock) on h.SubActivityId = u.SubActivityId  
                            left join CentralConfigRepl.tdrActivity.Activity a with (nolock) on u.ActivityId = a.ActivityId
                            where
                            fb_stat = 'Open'
                            and l.ln_chrg_code is null
                            and l.chrg_desc is not null
                            and u.SubActivity = 'LineItem'
                            and l.chrg_amt > 0
                            and f.fb_id like 'FBLL",CustData,"%'
                            group by f.owner_key,f.vend_labl,l.chrg_desc
                            order by f.owner_key,f.vend_labl asc",sep=""),na.string="")}
  else
  {sqlQuery(dbhandle, paste("select f.owner_key as 'owner_key'
                                   ,f.vend_labl
                            ,'' as 'col1'
                            ,'' as 'col2'
                            ,'' as 'col3'
                            ,l.chrg_desc as [LookupDesc]
                            ,'' as 'CanonChargeCode'
                            ,Replace(SYSTEM_USER,'FILEX\\Ruben.Felomino','yourname') as 'UserName'
                            ,CONVERT (DATE, GETDATE()) as 'UpdatedOn'
                            ,max(fb_key) as 'samplefb'
                            ,count(f.fb_key) as 'AffectedFB' 
                            from FAIL_RI FR (nolock)    
                            inner join outfb_ln l (nolock)  on FR.UNT_ID=l.FB_ID   
                            inner join FRGHT_BL f (nolock)  on FR.UNT_ID=f.FB_ID  
                            inner join INV_BAT i (nolock) on f.BAT_ID=i.BAT_ID 
                            inner join exceptions e (nolock) on f.bat_id=e.bat_id
                            where i.vend_labl <> 'NOSCAC'  
                            and i.vend_labl <> 'TRAXI' 
                            and  l.ln_chrg_code IS NULL 
                            and l.chrg_desc is not NULL
				                    and l.chrg_amt > 0
                            and f.fb_id like 'FBLL",CustData,"%'
                            group by f.owner_key,f.vend_labl,l.chrg_desc
                            order by f.owner_key,f.vend_labl asc",sep=""),na.string="")}
  
 
  #Reading of csv file into memory
  ChargeCodeTableMain <- read.csv("ChargeCodeTable.csv",header = T,stringsAsFactors = F)
  ChargeCodeTable <- subset(ChargeCodeTableMain,grepl(CustKey,ChargeCodeTableMain$OwnerKey) | grepl("0000",ChargeCodeTableMain$OwnerKey))
  ChargeDescInExceptions <- subset(res,grepl("1-",res$owner_key) | grepl("0-",res$owner_key))
  ChargeDescInExceptions <- ChargeDescInExceptions[which(ChargeDescInExceptions$vend_labl != 'NOSCAC'),]
  
  
  #Creating Subset Table
  charges <- ChargeCodeTable
  forlookup <- ChargeDescInExceptions["LookupDesc"]
  
  #Replicate first the column to cleanup
  charges$cleanupcharge <- charges$ChargeDescription
  forlookup$cleanupcharge <- forlookup$LookupDesc
  
  
  #Convert charges to string
  charges$cleanupcharge <-as.character(charges$cleanupcharge)
  forlookup$cleanupcharge <- as.character(forlookup$cleanupcharge)
  
  #Cleanup the data
  charges$cleanupcharge <-gsub("[^a-zA-Z]", "", charges$cleanupcharge)
  charges$cleanupcharge <-tolower(charges$cleanupcharge)
  forlookup$cleanupcharge <-gsub("[^a-zA-Z]", "", forlookup$cleanupcharge)
  forlookup$cleanupcharge <-tolower(forlookup$cleanupcharge)
  
  
  #create a function to look for each row of charge description to charge code table (aka line norm template)
  
  ClosestMatch = function(string, stringVector){
    
    distance = levenshteinSim(string, stringVector);
    stringVector[which.max(distance)]
    
  }
  
  
  #Lookup start
  forlookup$result <- sapply(forlookup$cleanupcharge,ClosestMatch,stringVector=charges$cleanupcharge)
  
  #Result
  forlookup$chargecode <- charges$ChargeCode[match(forlookup$result,charges$cleanupcharge)]
  forlookup$matchedchargedesc <- charges$ChargeDescription[match(forlookup$result,charges$cleanupcharge)]
  forlookup$PercentAccuracy <- round(1-stringdist(forlookup$LookupDesc,forlookup$matchedchargedesc,method = "jw"),digits = 2)
  forlookup$PercentAccuracyClean <- round(1-stringdist(forlookup$cleanupcharge,forlookup$result,method = "jw"),digits = 2)
  forlookup$chargecode <- ifelse(forlookup$PercentAccuracyClean < 0.75,"",as.character(forlookup$chargecode))
  ChargeDescInExceptions$CanonChargeCode <- forlookup$chargecode[match(ChargeDescInExceptions$LookupDesc,forlookup$LookupDesc)]
  ChargeDescInExceptions$AccPercentage <- forlookup$PercentAccuracyClean[match(ChargeDescInExceptions$LookupDesc,forlookup$LookupDesc)]
  ChargeDescInExceptions$MatchedChargeDesc <- forlookup$matchedchargedesc[match(ChargeDescInExceptions$LookupDesc,forlookup$LookupDesc)]
  ChargeDescAdd <- subset(ChargeDescInExceptions,ChargeDescInExceptions$AccPercentage >= 0.90)
  ChargeDescAddFinal <- ChargeDescAdd[,c(1,6,7)]
  names(ChargeDescAddFinal)[1] <- "OwnerKey"
  names(ChargeDescAddFinal)[2] <- "ChargeDescription"
  names(ChargeDescAddFinal)[3] <- "ChargeCode"
  ChargeCodeTableMain <- rbind(ChargeCodeTableMain,ChargeDescAddFinal)
  ChargeDescInExceptions[is.na(ChargeDescInExceptions)] <- ""
  ChargeDescInExceptions$UpdatedOn <- format(Sys.Date(),format = "%m/%d/%Y")
  ChargeCodeTableMain <- unique(ChargeCodeTableMain)
  write.csv(ChargeCodeTableMain,"ChargeCodeTable.csv",row.names=F)
  #write.csv(ChargeDescInExceptions,file = "ChargeDescwithChargeCode.csv",row.names = F)
  return(ChargeDescInExceptions)
  odbcClose(dbHandle)
}
app <- shinyApp(ui =fluidPage(
  titlePanel('Charge Desc Exceptions Generator'),
  sidebarLayout(
    sidebarPanel( 
      selectInput("val","Choose Customer",choices = dblist$DBName),
      fileInput("file","Load Template",
                accept=c('text/csv', 'text/comma-separated-values,text/plain'))),
    downloadButton('ChargeDescInExceptions','Download')
  )
),
server = function(input,output){
  output$result <- renderUI({
    lookup(input$val,input$text)
  })
    observe({
    file = input$file
    if (is.null(file)) {
      return(NULL)
    }
    infile = read.csv(file$datapath)
    chargecodetable <- read.csv("~/Unicorn/RWorkingFiles/LineNorm/ChargeCodeTable.csv",header = T,stringsAsFactors = F)
    infile <- infile[,c(1,6,7)]
    names(infile)[1] <- "OwnerKey"
    names(infile)[2] <- "ChargeDescription"
    names(infile)[3] <- "ChargeCode"
    ChargeCodeTable <- rbind(chargecodetable,infile)
    ChargeCodeTable <- unique(ChargeCodeTable)
    ChargeCodeTable <-  ChargeCodeTable[!(is.na(ChargeCodeTable$ChargeCode) | ChargeCodeTable$ChargeCode==""), ]
    write.csv(ChargeCodeTable,"ChargeCodeTable.csv",row.names=F)
    
  })
    output$ChargeDescInExceptions <- downloadHandler(
    filename = function(){
      paste(input$val,'_ChargeDescwithChargeCode_',format(Sys.Date(),'%d%b%y'),'.csv',sep="")
    },
    content = function(file) {
      write.csv(lookup(input$val), file,row.names=F)
    }
  )
}
)

