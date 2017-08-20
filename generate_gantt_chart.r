#install.packages("plotly")
#install.packages("RODBC")

## params:
# script 
pDebug = 0 
# report data
pStartDate = '20170820'
pEndDate = '20170820'
pGetSSISPackages = '1'
pGetStoredProcedures = '1'
pGetAgentJobs = '1'
pGurationMin = '60' # in seconds
# connection
serverAddress = 'localhost\\sql2016'
databaseName = 'SampleDWH'
## end of params


library(plotly)
library(RODBC)

myConnectionString <- paste('driver=SQL Server;server=',serverAddress,';database=',databaseName,';trusted_connection=true', sep="")
mySqlQuery <- paste("EXEC GanttReport @startDate = \'"
            ,  pStartDate 
            , '\', @endDate = \''
            , pEndDate
            , '\', @getSSISPackages = '
            , pGetSSISPackages
            , ', @getStoredProcedures = '
            , pGetStoredProcedures
            , ', @getAgentJobs = '
            , pGetAgentJobs
            , ', @durationMin = '
            , pGurationMin
         )

dbhandle <- odbcDriverConnect(myConnectionString)
res <- sqlQuery(dbhandle, mySqlQuery)
close(dbhandle)


## Read and order data
df <- res[order(-res$TaskName),]

# Convert to dates
#df$Start <- as.Date(df$Start, format = "yyyy-mm-dd HH:MM:SS.ssssss")
df$Start <- as.POSIXct(df$Start)

# Sample client name
client = "..."

{
  if (length(unique(df$Resource)) > 2)
  {
    cols <- RColorBrewer::brewer.pal(length(unique(df$Resource)), name = "Set3")
  }
  else if (length(unique(df$Resource)) == 2)
  {
    cols <- c("#FFFFB3", "#BEBADA")
  }
  else if (length(unique(df$Resource)) == 1)
  {
    cols <- c("#8DD3C7")
  }
  else 
  {
    
  }
}

# Choose colors based on number of resources
df$color <- factor(df$Resource, labels = cols)

# Initialize empty plot
p <- plot_ly()

# Each task is a separate trace
# Each trace is essentially a thick line plot
# x-axis ticks are dates and handled automatically

for(i in 1:(nrow(df) - 1)){
  p <- add_trace(p,
                 x = c(df$Start[i], df$Start[i] + df$Duration[i]),  # x0, x1
                 y = c(i, i),  # y0, y1
                 mode = "lines",
                 line = list(color = df$color[i], width = 10),
                 showlegend = F,
                 hoverinfo = "text",
                 
                 # Create custom hover text
                 
                 text = paste("Task: ", df$Task[i], "<br>",
                              "Start: ", df$Start[i], "<br>",
                              "End: ", df$Start[i] + df$Duration[i], "<br>",
                              "Duration: ", df$Duration[i], "seconds<br>",
                              "Resource: ", df$Resource[i], "<br>", 
                              df$AdditionalInfo[i]
                 ),
                 type="scatter"
                 
  )
  
  if (pDebug == 1)
  {
    print(i)
    print(df$Start[i])
    print(df$Start[i] + df$Duration[i])
    Sys.sleep(0.01)
  }
  
}


# Add information to plot and make the chart more presentable
customWidth = 1600

{
  if ((nrow(df) * 15) > 800)
  {
    customHeight =(nrow(df) * 15)
  }
  else 
  {
    customHeight = 800
  }
}

m <- list(l=100, r=50, b=((nrow(df) * 15)-650), t=30, pad=1)
p <- layout(p,
            # Axis options:
            # 1. Remove gridlines
            # 2. Customize y-axis tick labels and show task names instead of numbers
            
            xaxis = list(showgrid = F, tickfont = list(color = "#e6e6e6")),
            
            yaxis = list(showgrid = F, tickfont = list(color = "#e6e6e6"),
                         tickmode = "array", tickvals = 1:nrow(df), ticktext = unique(df$Task),
                         domain = c(0, 0.99),
                         x="0"
                         
            ),
            
            # Annotations
            margin=m,
            autosize=F,
            annotations = list(
              # Add total duration and total resources used
              # x and y coordinates are based on a domain of [0,1] and not
              # actual x-axis and y-axis values
              
              list(xref = "paper", yref = "paper",
                   x = 0.90, y = 0.90,
                   text = paste0("Total Duration: ", sum(df$Duration), " seconds<br>",
                                 "Total Resources: ", length(unique(df$Resource)), "<br>"),
                   font = list(color = "#ffff66", size = 12),
                   ax = 0, ay = 0,
                   align = "left"),
              
              # Add client name and title on top
              
              list(xref = "paper", yref = "paper",
                   x = 0.1, y = 1, xanchor = "left",
                   #text = paste0("Gantt Report (", nrow(res), " logs)"),
                   font = list(color = "#f2f2f2", size = 20, family = "Times New Roman"),
                   ax = 0, ay = 0,
                   align = "left")
            ),
            height=customHeight,
            width=customWidth,
            plot_bgcolor = "#333333",  # Chart area color
            paper_bgcolor = "#333333")  # Axis area color


p


