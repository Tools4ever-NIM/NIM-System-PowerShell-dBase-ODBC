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

    if ($Class -eq '') {

        if ($GetMeta) {
            #
            # Get all tables and views in database
            #

            Open-dBaseConnection $SystemParams

            $tables = $Global:dBaseConnection.getSchema("Tables")

            #
            # Output list of supported operations per table/view (named Class)
            #

            @(
                foreach ($t in $tables) {

                    $primary_key = '' #dBase you have you query primary index to find the primary key. TBD.
                    if ($t.TABLE_TYPE -ne 'Table') {
                        # Non-tables only support 'Read'
                        [ordered]@{
                            Class = $t.TABLE_NAME
                            Operation = 'Read'
                            'Source type' = $t.TABLE_TYPE
                            'Primary key' = $primary_key
                            'Supported operations' = 'R'
                        }
                    }
                    else {
                        [ordered]@{
                            Class = $t.TABLE_NAME
                            Operation = 'Create'
                        }

                        [ordered]@{
                            Class = $t.TABLE_NAME
                            Operation = 'Read'
                            'Source type' = $t.TABLE_TYPE
                            'Primary key' = $primary_key
                            'Supported operations' = "CR$(if ($primary_key) { 'UD' } else { '' })"
                        }

                        if ($primary_key) {
                            # Only supported if primary key is present
                            [ordered]@{
                                Class = $t.TABLE_NAME
                                Operation = 'Update'
                            }

                            [ordered]@{
                                Class = $t.TABLE_NAME
                                Operation = 'Delete'
                            }
                        }
                    }
                }
            )

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
            
            $columns = Get-SqlCommand-SelectColumnsInfo $Class

            switch ($Operation) {
                'Create' {
                    @{
                        semantics = 'create'
                        parameters = @(
                            $columns | ForEach-Object {
                                @{
                                    name = $_.ColumnName;
                                    #allowance = if ($_.is_identity -or $_.is_computed) { 'prohibited' } elseif (! $_.is_nullable) { 'mandatory' } else { 'optional' }
                                    allowance = 'optional'
                                }
                            }
                        )
                    }
                    break
                }

                'Read' {
                    @(
                        @{
                            name = 'where_clause'
                            type = 'textbox'
                            label = 'Filter (SQL where-clause)'
                            description = 'Applied SQL where-clause'
                            value = ''
                        }
                        @{
                            name = 'selected_columns'
                            type = 'grid'
                            label = 'Include columns'
                            description = 'Selected columns'
                            table = @{
                                rows = @($columns | ForEach-Object {
                                    @{
                                        name = $_.ColumnName
                                        config = @(
                                            #if ($_.IsKey) { 'Primary key' }
                                            #if ($_.is_identity)    { 'Auto identity' }
                                            #if ($_.is_computed)    { 'Computed' }
                                            #if ($_.is_nullable)    { 'Nullable' }
                                        ) -join ' | '
                                    }
                                })
                                settings_grid = @{
                                    selection = 'multiple'
                                    key_column = 'name'
                                    checkbox = $true
                                    filter = $true
                                    columns = @(
                                        @{
                                            name = 'name'
                                            display_name = 'Name'
                                        }
                                        @{
                                            name = 'config'
                                            display_name = 'Configuration'
                                        }
                                    )
                                }
                            }
                            value = @($columns | ForEach-Object { $_.ColumnName })
                        }
                    )
                    break
                }

                'Update' {
                    @{
                        semantics = 'update'
                        parameters = @(
                            $columns | ForEach-Object {
                                @{
                                    name = $_.ColumnName;
                                    #allowance = if ($_.is_primary_key) { 'mandatory' } else { 'optional' }
                                    allowance = 'optional'
                                }
                            }
                        )
                    }
                    break
                }

                'Delete' {
                    @{
                        semantics = 'delete'
                        parameters = @(
                            $columns | ForEach-Object {
                                if ($_.IsKey) {
                                    @{
                                        name = $_.ColumnName
                                        allowance = 'mandatory'
                                    }
                                }
                            }
                            @{
                                name = '*'
                                allowance = 'prohibited'
                            }
                        )
                    }
                    break
                }
            }

        }
        else {
            #
            # Execute function
            #

            Open-dBaseConnection $SystemParams

            if (! $Global:ColumnsInfoCache[$Class]) {
                $columns = Get-SqlCommand-SelectColumnsInfo $Class

                $Global:ColumnsInfoCache[$Class] = @{
                    primary_key = ''
                    identity_col = ''
                }
            }

            $primary_key  = $Global:ColumnsInfoCache[$Class].primary_key
            $identity_col = $Global:ColumnsInfoCache[$Class].identity_col

            $function_params = ConvertFrom-Json2 $FunctionParams

            $command = $null

            $projection = if ($function_params['selected_columns'].count -eq 0) { '*' } else { @($function_params['selected_columns'] | ForEach-Object { "`"$_`"" }) -join ', ' }
            log debug $projection
            log debug $Operation
            switch ($Operation) {
                'Create' {
                    $selection = if ($identity_col) {
                                     "[$identity_col] = SCOPE_IDENTITY()"
                                 }
                                 elseif ($primary_key) {
                                     "[$primary_key] = '$($function_params[$primary_key])'"
                                 }
                                 else {
                                     @($function_params.Keys | ForEach-Object { "`"$_`" = '$($function_params[$_])'" }) -join ' AND '
                                 }

                    $command = "INSERT INTO $Class ($(@($function_params.Keys | ForEach-Object { '"'+$_+'"' }) -join ', ')) VALUES ($(@($function_params.Keys | ForEach-Object { "$(if ($function_params[$_] -ne $null) { "'$($function_params[$_])'" } else { 'null' })" }) -join ', ')); SELECT TOP(1) $projection FROM $Class WHERE $selection"
                    break
                }

                'Read' {
                    $selection = if ($function_params['where_clause'].length -eq 0) { '' } else { " WHERE $($function_params['where_clause'])" }
                    log debug $selection
                    $command = "SELECT $projection FROM $Class $selection"
                    log debug $command
                    break
                }

                'Update' {
                    $command = "UPDATE TOP(1) $Class SET $(@($function_params.Keys | ForEach-Object { if ($_ -ne $primary_key) { "[$_] = $(if ($function_params[$_] -ne $null) { "'$($function_params[$_])'" } else { 'null' })" } }) -join ', ') WHERE [$primary_key] = '$($function_params[$primary_key])'; SELECT TOP(1) [$primary_key], $(@($function_params.Keys | ForEach-Object { if ($_ -ne $primary_key) { "[$_]" } }) -join ', ') FROM $Class WHERE [$primary_key] = '$($function_params[$primary_key])'"
                    break
                }

                'Delete' {
                    $command = "DELETE TOP(1) $Class WHERE [$primary_key] = '$($function_params[$primary_key])'"
                    break
                }
            }

            if ($command) {
                Log debug $command
                LogIO info ($command -split ' ')[0] -In -Command $command

                if ($Operation -eq 'Read') {
                    # Streamed output
                    Invoke-dBaseCommand $command
                }
                else {
                    # Log output
                    $rv = Invoke-dBaseCommand $command
                    LogIO info ($command -split ' ')[0] -Out $rv

                    $rv
                }
            }

        }

    }

    Log info "Done"
}


#
# Helper functions
#

function Invoke-dBaseCommand {
    param (
        [string] $Command
    )

    function Invoke-dBaseCommand-ExecuteReader {
        param (
            [string] $Command
        )
        log debug $Command     
        $sql_command  = New-Object System.Data.Odbc.OdbcCommand($Command, $Global:dBaseConnection)
        $data_adapter = New-Object System.Data.Odbc.OdbcDataAdapter($sql_command)
        $data_table   = New-Object System.Data.DataTable
        $data_adapter.Fill($data_table) | Out-Null

        # Output data
        $data_table.Rows | Select $data_table.Columns.ColumnName

        log debug $data_table.Columns

        $data_table.Dispose()
        $data_adapter.Dispose()
        $sql_command.Dispose()
    }
    $Command = ($Command -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }) -join ' '

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
    LOG info $connection_string

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
