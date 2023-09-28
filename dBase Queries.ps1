$Log_MaskableKeys = @(
    'password'
)


#
# System functions
#

function Idm-SystemInfo {
    param (
        # Operations
        [switch] $Connection,
        [switch] $TestConnection,
        [switch] $Configuration,
        # Parameters
        [string] $ConnectionParams
    )

    Log info "-Connection=$Connection -TestConnection=$TestConnection -Configuration=$Configuration -ConnectionParams='$ConnectionParams'"
    
    if ($Connection) {
        @(
            @{
                name = 'path'
                type = 'textbox'
                label = 'Path'
                value = ''
            }
            @{
                name = 'file_dsn'
                type = 'textbox'
                label = 'File DSN Path'
                value = ''
            }
			@{
                name = 'table_1_name'
                type = 'textbox'
                label = 'Query 1 - Name of Table'
                description = ''
            }
            @{
                name = 'table_1_query'
                type = 'textbox'
                label = 'Query 1 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_2_name'
                type = 'textbox'
                label = 'Query 2 - Name of Table'
                description = ''
            }
            @{
                name = 'table_2_query'
                type = 'textbox'
                label = 'Query 2 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_3_name'
                type = 'textbox'
                label = 'Query 3 - Name of Table'
                description = ''
            }
            @{
                name = 'table_3_query'
                type = 'textbox'
                label = 'Query 3 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_4_name'
                type = 'textbox'
                label = 'Query 4 - Name of Table'
                description = ''
            }
            @{
                name = 'table_4_query'
                type = 'textbox'
                label = 'Query 4 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_5_name'
                type = 'textbox'
                label = 'Query 5 - Name of Table'
                description = ''
            }
            @{
                name = 'table_5_query'
                type = 'textbox'
                label = 'Query 5 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_6_name'
                type = 'textbox'
                label = 'Query 6 - Name of Table'
                description = ''
            }
            @{
                name = 'table_6_query'
                type = 'textbox'
                label = 'Query 6 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_7_name'
                type = 'textbox'
                label = 'Query 7 - Name of Table'
                description = ''
            }
            @{
                name = 'table_7_query'
                type = 'textbox'
                label = 'Query 7 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_8_name'
                type = 'textbox'
                label = 'Query 8 - Name of Table'
                description = ''
            }
            @{
                name = 'table_8_query'
                type = 'textbox'
                label = 'Query 8 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_9_name'
                type = 'textbox'
                label = 'Query 9 - Name of Table'
                description = ''
            }
            @{
                name = 'table_9_query'
                type = 'textbox'
                label = 'Query 9 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_10_name'
                type = 'textbox'
                label = 'Query 10 - Name of Table'
                description = ''
            }
            @{
                name = 'table_10_query'
                type = 'textbox'
                label = 'Query 10 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_11_name'
                type = 'textbox'
                label = 'Query 11 - Name of Table'
                description = ''
            }
            @{
                name = 'table_11_query'
                type = 'textbox'
                label = 'Query 11 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_12_name'
                type = 'textbox'
                label = 'Query 12 - Name of Table'
                description = ''
            }
            @{
                name = 'table_12_query'
                type = 'textbox'
                label = 'Query 12 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_13_name'
                type = 'textbox'
                label = 'Query 13 - Name of Table'
                description = ''
            }
            @{
                name = 'table_13_query'
                type = 'textbox'
                label = 'Query 13 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_14_name'
                type = 'textbox'
                label = 'Query 14 - Name of Table'
                description = ''
            }
            @{
                name = 'table_14_query'
                type = 'textbox'
                label = 'Query 14 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_15_name'
                type = 'textbox'
                label = 'Query 15 - Name of Table'
                description = ''
            }
            @{
                name = 'table_15_query'
                type = 'textbox'
                label = 'Query 15 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_16_name'
                type = 'textbox'
                label = 'Query 16 - Name of Table'
                description = ''
            }
            @{
                name = 'table_16_query'
                type = 'textbox'
                label = 'Query 16 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_17_name'
                type = 'textbox'
                label = 'Query 17 - Name of Table'
                description = ''
            }
            @{
                name = 'table_17_query'
                type = 'textbox'
                label = 'Query 17 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_18_name'
                type = 'textbox'
                label = 'Query 18 - Name of Table'
                description = ''
            }
            @{
                name = 'table_18_query'
                type = 'textbox'
                label = 'Query 18 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_19_name'
                type = 'textbox'
                label = 'Query 19 - Name of Table'
                description = ''
            }
            @{
                name = 'table_19_query'
                type = 'textbox'
                label = 'Query 19 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_20_name'
                type = 'textbox'
                label = 'Query 20 - Name of Table'
                description = ''
            }
            @{
                name = 'table_20_query'
                type = 'textbox'
                label = 'Query 20 - SQL Statement'
                description = ''
            }
        )
    }

    if ($TestConnection) {
        Open-dBaseConnection $ConnectionParams
    }

    if ($Configuration) {
        @()
    }

    Log info "Done"
}


function Idm-OnUnload {
    Close-dBaseConnection
}


#
# CRUD functions
#

$ColumnsInfoCache = @{}
$SqlInfoCache = @{}

function Fill-SqlInfoCache {
    param (
        [switch] $Force,
        [string] $Query,
        [string] $Class
    )
	Log info "-Class='$Class' -Query='$Query'"

    # Refresh cache
    $sql_command = New-dBaseCommand $Query

    $result = (Invoke-dBaseCommand $sql_command) | Get-Member -MemberType Properties | Select-Object Name

    Dispose-dBaseCommand $sql_command

    $objects = New-Object System.Collections.ArrayList
    $object = @{}
    # Process in one pass
    foreach ($row in $result) {
            $object = @{
                full_name = $Class
                type      = 'Query'
                columns   = New-Object System.Collections.ArrayList
            }

        $object.columns.Add(@{
            name           = $row.Name
            is_primary_key = $false
            is_identity    = $false
            is_computed    = $false
            is_nullable    = $true
        }) | Out-Null
    }

    if ($object.full_name -ne $null) {
        $objects.Add($object) | Out-Null
    }
    @($objects)
}

function Idm-Dispatcher {
    param (
        # Optional Class/Operation
        [string] $Class,
        [string] $Operation,
        # Mode
        [switch] $GetMeta,
        # Parameters
        [string] $SystemParams,
        [string] $FunctionParams
    )

    Log info "-Class='$Class' -Operation='$Operation' -GetMeta=$GetMeta -SystemParams='$SystemParams' -FunctionParams='$FunctionParams'"
    $connection_params = ConvertFrom-Json2 $SystemParams

    if ($Class -eq '') {

        if ($GetMeta) {
            #
            # Get all tables and views in database
            #

            Open-dBaseConnection $SystemParams
            
            #
            # Output list of supported operations per table/view (named Class)
            #
            for (($i = 0), ($j = 0); $i -lt 21; $i++)
            {
                if($connection_params."table_$($i)_name".length -gt 0)
                {
                    @(
                        [ordered]@{
                            Class = $connection_params."table_$($i)_name"
                            Operation = 'Read'
                            'Source type' = 'Query'
                            'Primary key' = ''
                            'Supported operations' = 'R'
                            'Query' = $connection_params."table_$($i)_query"
                        }
                    )
                }
            }
        }
        else {
            # Purposely no-operation.
        }

    }
    else {

        if ($GetMeta) {
            #
            # Get meta data
            #
            Open-dBaseConnection $SystemParams
            @()
        }
        else {
            #
            # Execute function
            #

            Open-dBaseConnection $SystemParams

            for (($i = 0), ($j = 0); $i -lt 21; $i++)
            {
                if($connection_params."table_$($i)_name" -eq $class)
                {
                    $class_query = ($connection_params."table_$($i)_query" -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }) -join ' '
                    break
                }
            }
            
            $columns = Fill-SqlInfoCache -Query $class_query
            $Global:ColumnsInfoCache[$Class] = @{
                primary_keys = @($columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name })
                identity_col = @($columns | Where-Object { $_.is_identity    } | ForEach-Object { $_.name })[0]
            }

            $primary_keys = $Global:ColumnsInfoCache[$Class].primary_keys
            $identity_col = $Global:ColumnsInfoCache[$Class].identity_col

            $function_params = ConvertFrom-Json2 $FunctionParams

            # Replace $null by [System.DBNull]::Value
            $keys_with_null_value = @()
            foreach ($key in $function_params.Keys) { if ($function_params[$key] -eq $null) { $keys_with_null_value += $key } }
            foreach ($key in $keys_with_null_value) { $function_params[$key] = [System.DBNull]::Value }

            $sql_command = New-dBaseCommand

            $projection = if ($function_params['selected_columns'].count -eq 0) { '*' } else { @($function_params['selected_columns'] | ForEach-Object { "[$_]" }) -join ', ' }

            switch ($Operation) {
                'Read' {
					LogIO info "Query: $class_query"
                    $sql_command.CommandText = $class_query
                    break
                }
            }

            if ($sql_command.CommandText) {
                if ($Operation -eq 'Read') {
                    # Streamed output
					Invoke-dBaseCommand $sql_command
                }
                else {
					$rv = Invoke-dBaseCommand $sql_command
                    $rv
                }
            }
            Dispose-dBaseCommand $sql_command
        }
    }
    Log info "Done"
}

#
# Helper functions
#

function New-dBaseCommand {
	param (
		[string] $CommandText
	)
	New-Object System.Data.Odbc.OdbcCommand($CommandText, $Global:dBaseConnection)
}

function Dispose-dBaseCommand {
    param (
        [System.Data.Odbc.OdbcCommand] $Command
    )
    $Command.Dispose()
}

function Invoke-dBaseCommand {
    param (
        [System.Data.Odbc.OdbcCommand] $Command
    )

    function Invoke-dBaseCommand-ExecuteReader {
        param (
            [System.Data.Odbc.OdbcCommand] $Command
        )

        $data_adapter = New-Object System.Data.Odbc.OdbcDataAdapter($Command)
        $data_table   = New-Object System.Data.DataTable
        $data_adapter.Fill($data_table) | Out-Null

        # Output data
        $data_table.Rows | Select $data_table.Columns.ColumnName

        log debug $data_table.Columns

        $data_table.Dispose()
        $data_adapter.Dispose()
        $Command.Dispose()
    }

    try {
        Invoke-dBaseCommand-ExecuteReader $Command
    }
    catch {
        Log error "Failed: $_"
        Write-Error $_
    }
}

function Open-dBaseConnection {
    param (
        [string] $ConnectionParams
    )

    $connection_params = ConvertFrom-Json2 $ConnectionParams
    $connection_string =  "CollatingSequence=ASCII;DBQ=$($connection_params.path);DefaultDir=$($connection_params.path);Deleted=0;Driver={Microsoft Access dBASE Driver (*.dbf, *.ndx, *.mdx)};DriverId=533;FIL=dBase 5.0;FILEDSN=$($connection_params.file_dsn);MaxBufferSize=2048;MaxScanRows=8;PageTimeout=5;SafeTransactions=0;Statistics=0;Threads=3;UID=admin111;UserCommitSync=Yes;"
    Log info $connection_string

    if ($Global:dBaseConnection -and $connection_string -ne $Global:dBaseConnectionString) {
        Log info "dBaseConnection connection parameters changed"
        Close-dBaseConnection
    }

    if ($Global:dBaseConnection -and $Global:dBaseConnection.State -ne 'Open') {
        Log warn "dBaseConnection State is '$($Global:dBaseConnection.State)'"
        Close-dBaseConnection
    }

    if ($Global:dBaseConnection) {
        Log debug "Reusing dBaseConnection"
    }
    else {
        Log info "Opening dBaseConnection '$connection_string'"

        try {
            $connection = (new-object System.Data.Odbc.OdbcConnection);
            $connection.connectionstring = $connection_string
            $connection.open();

            $Global:dBaseConnection       = $connection
            $Global:dBaseConnectionString = $connection_string

            $Global:ColumnsInfoCache = @{}
        }
        catch {
            Log warn "Failed: $_"
            #Write-Error $_
        }

        Log info "Done"
    }
}

function Get-SqlCommand-SelectColumnsInfo {
    param (
        [string] $Table
    )
    Log info "Get Columns [$($Table)]"
    $Command = "SELECT TOP 1 * FROM $($Table)"
    log debug $Command
    $sql_command = New-Object System.Data.Odbc.OdbcCommand($Command, $Global:dBaseConnection)
    $reader = $sql_command.ExecuteReader()
    $reader.GetSchemaTable();
}

function Close-dBaseConnection {
    if ($Global:dBaseConnection) {
        Log info "Closing dBaseConnection"

        try {
            $Global:dBaseConnection.Close()
            $Global:dBaseConnection = $null
        }
        catch {
            # Purposely ignoring errors
        }

        Log info "Done"
    }
}