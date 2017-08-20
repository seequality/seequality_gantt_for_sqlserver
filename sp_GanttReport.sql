CREATE PROCEDURE [dbo].[GanttReport]  
(
	@startDate DATE 
	,@endDate DATE 
	,@getSSISPackages BIT = 1
	,@getStoredProcedures BIT = 1
	,@getAgentJobs BIT = 1
	,@durationMin INT = 0
)

AS 

SET NOCOUNT ON

BEGIN 

	CREATE TABLE #results
	(
		[TaskName] [NVARCHAR](500) NULL,
		[Start] [DATETIME] NULL,
		[Duration] [INT] NULL,
		[Resource] [VARCHAR](20) NOT NULL,
		[AdditionalInfo] [VARCHAR](max) NULL
	) 

	IF @getAgentJobs = 1
	BEGIN 

		;WITH cte AS 
		(
			SELECT 
				j.job_id AS 'AgentJobID',
				j.name AS 'AgentJobName',
				j.version_number AS 'AgentJobVersion',
				s.step_id AS 'AgentStepID',
				s.step_name AS 'AgentStepName',
				s.subsystem AS 'AgentStepSubsystem',
				s.step_uid AS 'AgentStepUID',
				h.instance_id AS 'AgentRunInstanceID',
				CASE h.run_status 
					WHEN 0 THEN 'Failed'
					WHEN 1 THEN 'Succeeded'
					WHEN 2 THEN 'Retry'
					WHEN 3 THEN 'Canceled'
				END AS 'AgentRunRunStatus',
				msdb.dbo.agent_datetime(run_date, run_time) AS 'AgentStepCalendarDate',
				msdb.dbo.agent_datetime(run_date, run_time) AS 'AgentStepStartTime',
				DATEADD(SECOND, ((h.run_duration/1000000)*86400) + (((h.run_duration-((h.run_duration/1000000)*1000000))/10000)*3600) + (((h.run_duration-((h.run_duration/10000)*10000))/100)*60) + (h.run_duration-(h.run_duration/100)*100),  msdb.dbo.agent_datetime(run_date, run_time)) AS 'AgentStepEndTime',
				'Agent' AS 'AgentLogType'
			FROM msdb.dbo.sysjobs j 
			INNER JOIN msdb.dbo.sysjobsteps s 
				ON j.job_id = s.job_id
			INNER JOIN msdb.dbo.sysjobhistory h 
				ON s.job_id = h.job_id 
				AND s.step_id = h.step_id 
			WHERE 
				CONVERT(DATE,CONVERT(VARCHAR(20),run_date)) BETWEEN @startDate AND @endDate
		), cte2 AS 
		(
			SELECT
				a.AgentJobID ,
				a.AgentJobName ,
				a.AgentJobVersion ,
				a.AgentStepID ,
				a.AgentStepName ,
				a.AgentStepSubsystem ,
				a.AgentStepUID ,
				a.AgentRunInstanceID ,
				a.AgentRunRunStatus ,
				a.AgentStepCalendarDate ,
				a.AgentStepStartTime ,
				a.AgentStepEndTime ,
				a.AgentLogType,
				'Job ' + a.AgentJobName + ' ' + a.AgentStepName + ' (' + CONVERT(VARCHAR(100),a.AgentRunInstanceID) + ')' AS TaskName,
				DATEDIFF(SECOND, a.AgentStepStartTime, a.AgentStepEndTime) AS Duration,
				DATEDIFF(MINUTE, a.AgentStepStartTime, a.AgentStepEndTime) AS DurationMin,
				a.AgentStepStartTime AS 'Start',
				'Job' AS [Resource],
				'<br>' + 'AgentJobID' + ': ' + CONVERT(VARCHAR(100),ISNULL(a.AgentJobID,'') )
				+ '<br>' + 'AgentJobName' + ': ' + CONVERT(VARCHAR(100),ISNULL(a.AgentJobName ,''))
				+ '<br>' + 'AgentJobVersion' + ': ' + CONVERT(VARCHAR(100),ISNULL(a.AgentJobVersion,'') )
				+ '<br>' + 'AgentStepID' + ': ' + CONVERT(VARCHAR(100),ISNULL(a.AgentStepID ,''))
				+ '<br>' + 'AgentStepName' + ': ' + CONVERT(VARCHAR(100),ISNULL(a.AgentStepName,'') )
				+ '<br>' + 'AgentStepSubsystem' + ': ' + CONVERT(VARCHAR(100),ISNULL(a.AgentStepSubsystem,'') )
				+ '<br>' + 'AgentStepUID' + ': ' + ISNULL(CONVERT(VARCHAR(100),a.AgentStepUID) ,'')
				+ '<br>' + 'AgentRunInstanceID' + ': ' + CONVERT(VARCHAR(100),ISNULL(a.AgentRunInstanceID ,''))
				+ '<br>' + 'AgentRunRunStatus' + ': ' + CONVERT(VARCHAR(100),ISNULL(a.AgentRunRunStatus ,''))
				+ '<br>' + 'AgentStepStartTime' + ': ' + CONVERT(VARCHAR(100),ISNULL(a.AgentStepStartTime ,''))
				+ '<br>' + 'AgentStepEndTime' + ': ' + CONVERT(VARCHAR(100),ISNULL(a.AgentStepEndTime ,''))
				AS AdditionalInfo
			FROM cte a
		)
		INSERT INTO #results 
		(   
			TaskName ,
			Start ,
			Duration ,
			Resource ,
			AdditionalInfo
		)
		SELECT 
			TaskName, 
			Start, 
			Duration, 
			[Resource], 
			AdditionalInfo 
		FROM cte2

	END 

	IF @getSSISPackages = 1
	BEGIN 

	;WITH cte AS 
	(
		SELECT 
			ex.package_name, 
			ex.package_path,
			s.start_time, 
			s.end_time, 
			s.execution_duration/1000 AS execution_duration, 
			CASE s.execution_result
				WHEN 0 THEN 'Success'
				WHEN 1 THEN 'Failure'
				WHEN 2 THEN 'Completion'
				WHEN 3 THEN 'Cancelled'
				ELSE 'Unknown'
			END AS ExecutionResult,
			e.execution_id,
			ex.executable_id,
			s.statistics_id
		FROM [SSISDB].[catalog].[executable_statistics] s
		INNER JOIN SSISDB.catalog.executions e ON s.execution_id = e.execution_id
		INNER JOIN SSISDB.catalog.executables ex ON s.execution_id = ex.execution_id AND s.executable_id = ex.executable_id
		WHERE ex.package_path = '\Package'
		AND s.execution_result = 0
		AND CONVERT(DATE,s.start_time) BETWEEN @startDate AND @endDate
	)
	INSERT INTO #results 
	(   
		TaskName ,
		Start ,
		Duration ,
		Resource ,
		AdditionalInfo
	)
	SELECT 
		a.package_name + ' ' + CONVERT(VARCHAR(25),a.execution_id) + ' ' + CONVERT(VARCHAR(25),a.statistics_id) AS TaskName,
		a.start_time,
		a.execution_duration AS Duration,
		'Package' AS [Resource],
		'<br>' + 'package_name' + ': ' + CONVERT(VARCHAR(500),ISNULL(a.package_name,''))
		+ '<br>' + 'package_path' + ': ' + CONVERT(VARCHAR(500),ISNULL(a.package_path,'')) 
		+ '<br>' + 'start_time' + ': ' + CONVERT(VARCHAR(50),ISNULL(a.start_time,'')) 
		+ '<br>' + 'end_time' + ': ' + CONVERT(VARCHAR(50),ISNULL(a.end_time,'')) 
		+ '<br>' + 'execution_duration' + ': ' + CONVERT(VARCHAR(50),ISNULL(a.execution_duration,'')) 
		+ '<br>' + 'ExecutionResult' + ': ' + CONVERT(VARCHAR(50),ISNULL(a.ExecutionResult,'')) 
		+ '<br>' + 'execution_id' + ': ' + CONVERT(VARCHAR(50),ISNULL(a.execution_id,'')) 
		+ '<br>' + 'executable_id' + ': ' + CONVERT(VARCHAR(50),ISNULL(a.executable_id,'')) 
		+ '<br>' + 'statistics_id' + ': ' + CONVERT(VARCHAR(50),ISNULL(a.statistics_id,'')) 
		AS AdditionalInfo
	FROM cte a

	END 

	IF @getStoredProcedures = 1
	BEGIN 

	;WITH cte AS 
	(
		SELECT 
			a.spRunLogID AS 'ProcProcRunLogID',
			a.packageRunID AS 'ProcPacakgeRunID',
			a.spName AS 'ProcProcName',
			a.spRunStatus AS 'ProcRunStatus',
			a.startTime AS 'ProcStartTime',
			a.endTime AS 'ProcEndTime',
			'Proc' AS 'ProcLogType',
			'Proc ' + a.spName + ' (' + CONVERT(VARCHAR(100),a.spRunLogID) + ')' AS TaskName,
			DATEDIFF(SECOND, a.startTime, a.endTime) AS Duration,
			DATEDIFF(MINUTE, a.startTime, a.endTime) AS DurationMin,
			a.startTime AS 'Start',
			'Proc' AS [Resource],
			  '<br>' + 'ProcProcRunLogID' + ': ' + CONVERT(VARCHAR(100),ISNULL(a.spRunLogID,''))
			+ '<br>' + 'ProcPacakgeRunID' + ': ' + CONVERT(VARCHAR(100),ISNULL(a.packageRunID,'') )
			+ '<br>' + 'ProcProcName' + ': ' + ISNULL(a.spName,'') 
			+ '<br>' + 'ProcRunStatus' + ': ' + ISNULL(a.spRunStatus ,'')
			+ '<br>' + 'ProcStartTime' + ': ' + CONVERT(VARCHAR(100),ISNULL(a.startTime,'') )
			+ '<br>' + 'ProcEndTime' + ': ' + CONVERT(VARCHAR(100),ISNULL(a.endTime,'') )
			+ '<br>' + 'Duration' + ': ' + ISNULL(CONVERT(VARCHAR(25),DATEDIFF(SECOND, a.startTime, a.endTime)),'')
			+ '<br>' + 'DurationMin' + ': ' + ISNULL(CONVERT(VARCHAR(25),DATEDIFF(MINUTE, a.startTime, a.endTime)),'')
			AS AdditionalInfo
		FROM dbo.spRunLog a
		WHERE CONVERT(DATE,a.startTime) BETWEEN @startDate AND @endDate
		AND a.spRunStatus = 'OK'
	)
	INSERT INTO #results 
	(   
		TaskName ,
		Start ,
		Duration ,
		Resource ,
		AdditionalInfo
	)
	SELECT 
		TaskName,
		Start,
		Duration,
		[Resource],
		AdditionalInfo
	FROM cte
	END 

	SELECT 
		CONVERT(BIGINT, 1000000 + ROW_NUMBER() OVER(ORDER BY Start ASC)) AS TaskName ,
		Start ,
		Duration ,
		Resource ,
		AdditionalInfo
	FROM #results
	WHERE 1 = 1
	AND Duration >= @durationMin
	ORDER BY Start DESC

END

GO