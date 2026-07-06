<#

SCRIPT IS IN BETA. ALWAYS RECHECK THE OUTPUT 

.SYNOPSIS
    Finds a user in Active Directory and checks each clause of USERSFILTER
    against that user, reporting which parts of the filter pass and which fail.

.DESCRIPTION
    The user is located by the attribute named in $Attribute whose value is
    $User (e.g. samaccountname = 12345678). The compound LDAP filter in
    $UsersFilter is split into its atomic leaf clauses, and every clause is
    re-tested individually against the same user by asking Active Directory to
    evaluate  (&($Attribute=$User)<clause>).  Letting AD evaluate each clause
    means bitwise matching rules (e.g. userAccountControl:1.2.840.113556.1.4.803:=2)
    and memberOf nested-group semantics are handled correctly.
#>

# --- Configuration ----------------------------------------------------------

# Path to the config file. If set and the file exists, settings are read from it.
# If empty (or the file is missing), the hardcoded values below are used.
$ConfigPath   = ''

# The user to look up. Always set manually.
$User         = '12345678'

# Hardcoded fallback values (used when $ConfigPath is empty / file not found).
$Attribute    = 'samaccountname'
$WorkMailAttr = 'mail'
$RequiredProperties = @()
$Ldap         = ''
$UsersFilter  = ''

# ---------------------------------------------------------------------------

function Read-Config {
    param([string]$Path)

    $result = @{}

    foreach ($raw in Get-Content -LiteralPath $Path) {
        $line = $raw.Trim()
        if ($line -eq '' -or $line.StartsWith('#')) { continue }

        $sep = $line.IndexOf('=')
        if ($sep -lt 1) { continue }

        $key   = $line.Substring(0, $sep).Trim()
        $value = $line.Substring($sep + 1).Trim()
        if ($value -eq '') { continue }

        if (-not $result.ContainsKey($key)) {
            $result[$key] = New-Object System.Collections.Generic.List[string]
        }
        $result[$key].Add($value)
    }

    return $result
}

if ($ConfigPath -and (Test-Path -LiteralPath $ConfigPath)) {
    Write-Host "Config    : $ConfigPath" -ForegroundColor Cyan
    $cfg = Read-Config $ConfigPath

    if ($cfg.ContainsKey('LDAP'))        { $Ldap        = $cfg['LDAP'][0] }
    if ($cfg.ContainsKey('UsersFilter')) { $UsersFilter = $cfg['UsersFilter'][0] }

    # Login attribute: PropertyLoginName, otherwise userPrincipalName.
    if ($cfg.ContainsKey('PropertyLoginName')) {
        $Attribute = $cfg['PropertyLoginName'][0]
    } else {
        $Attribute = 'userPrincipalName'
    }

    # Work mail: PropertyWorkMail, otherwise the AD 'mail' attribute.
    if ($cfg.ContainsKey('PropertyWorkMail')) {
        $WorkMailAttr = $cfg['PropertyWorkMail'][0]
    } else {
        $WorkMailAttr = 'mail'
    }

    if ($cfg.ContainsKey('RequiredProperty')) {
        $RequiredProperties = @($cfg['RequiredProperty'])
    }
} elseif ($ConfigPath) {
    Write-Host "Config    : $ConfigPath not found - using hardcoded values." -ForegroundColor Yellow
} else {
    Write-Host "Config    : no path set - using hardcoded values." -ForegroundColor Cyan
}

# ---------------------------------------------------------------------------

function New-Searcher {
    param([string]$Filter)

    $root     = New-Object System.DirectoryServices.DirectoryEntry($Ldap)
    $searcher = New-Object System.DirectoryServices.DirectorySearcher($root)
    $searcher.Filter     = $Filter
    $searcher.PageSize   = 1000
    $searcher.SearchScope = 'Subtree'
    return $searcher
}

function Escape-LdapValue {
    # Escape the value used in the identity filter, per RFC 4515.
    param([string]$Value)

    $Value.Replace('\', '\5c').
           Replace('(', '\28').
           Replace(')', '\29').
           Replace('*', '\2a').
           Replace([string][char]0, '\00')
}

# LDAP_MATCHING_RULE_IN_CHAIN - resolves nested group membership.
$NestedRuleOid = '1.2.840.113556.1.4.1941'

function Convert-NestedMembership {
    # Groups under OU=DynamicGroups are checked via nested (in-chain) membership.
    param([string]$Leaf)

    if ($Leaf -match '(?i)\(memberof=' -and $Leaf -match '(?i)OU=DynamicGroups') {
        return $Leaf -replace '(?i)\(memberof=', "(memberOf:${NestedRuleOid}:="
    }
    return $Leaf
}

function Get-LeafClauses {
    <#
        Extract atomic clauses from a compound LDAP filter. A leaf clause is a
        parenthesised group that contains no nested parentheses, e.g.
        (objectClass=user) or (memberOf=CN=...). A leaf that is immediately
        preceded by '!' is a negation and is returned wrapped as (!(<leaf>)).
    #>
    param([string]$Filter)

    $clauses = New-Object System.Collections.Generic.List[string]
    $matches = [regex]::Matches($Filter, '\([^()]+\)')

    foreach ($m in $matches) {
        $leaf = $m.Value
        $negated = ($m.Index -gt 0 -and $Filter[$m.Index - 1] -eq '!')

        # Normalise accidental ", " spacing inside DNs (keeps real value spaces).
        $leaf = $leaf -replace ',\s+', ','

        $leaf = Convert-NestedMembership $leaf

        if ($negated) {
            $leaf = "(!$leaf)"
        }

        $clauses.Add($leaf)
    }

    return $clauses
}

# --- 1. Locate the user ----------------------------------------------------

$escapedUser  = Escape-LdapValue $User
$identity     = "($Attribute=$escapedUser)"

Write-Host "LDAP root : $Ldap"
Write-Host "Identity  : $identity"
Write-Host ""

$userSearcher = New-Searcher $identity

$attrsToLoad = New-Object System.Collections.Generic.List[string]
$attrsToLoad.Add('distinguishedName')
$attrsToLoad.Add($WorkMailAttr)
foreach ($p in $RequiredProperties) { if (-not $attrsToLoad.Contains($p)) { $attrsToLoad.Add($p) } }
foreach ($a in $attrsToLoad) { $userSearcher.PropertiesToLoad.Add($a) | Out-Null }

$found = $userSearcher.FindOne()

if ($null -eq $found) {
    Write-Host "User NOT FOUND by $Attribute=$User" -ForegroundColor Red
    return
}

$dn = $found.Properties['distinguishedname'][0]
Write-Host "User found: $dn" -ForegroundColor Green
Write-Host ""

# --- Account attribute values ----------------------------------------------

function Get-PropValues {
    param($SearchResult, [string]$Name)

    $key = $Name.ToLower()
    if ($SearchResult.Properties.Contains($key) -and $SearchResult.Properties[$key].Count -gt 0) {
        return @($SearchResult.Properties[$key])
    }
    return @()
}

$mailValues = Get-PropValues $found $WorkMailAttr
if ($mailValues.Count -gt 0) {
    Write-Host ("WorkMail ({0}): {1}" -f $WorkMailAttr, ($mailValues -join ', ')) -ForegroundColor Green
} else {
    Write-Host ("WorkMail ({0}): <no value>" -f $WorkMailAttr) -ForegroundColor Yellow
}

if ($RequiredProperties.Count -gt 0) {
    Write-Host ""
    Write-Host "RequiredProperty:"
    foreach ($p in $RequiredProperties) {
        $vals = Get-PropValues $found $p
        if ($vals.Count -gt 0) {
            Write-Host ("  present  {0} = {1}" -f $p, ($vals -join ', ')) -ForegroundColor Green
        } else {
            Write-Host ("  MISSING  {0}" -f $p) -ForegroundColor Red
        }
    }
}
Write-Host ""

# --- 2. Test each leaf clause against that user ----------------------------

$clauses = Get-LeafClauses $UsersFilter

Write-Host ("Checking {0} clause(s):" -f $clauses.Count)
Write-Host ("-" * 70)

$passed = 0
$failed = 0

foreach ($clause in $clauses) {
    $testFilter = "(&$identity$clause)"

    try {
        $hit = (New-Searcher $testFilter).FindOne()
    } catch {
        Write-Host ("ERROR  {0}" -f $clause) -ForegroundColor Yellow
        Write-Host ("       {0}" -f $_.Exception.Message) -ForegroundColor Yellow
        continue
    }

    if ($null -ne $hit) {
        Write-Host ("PASS   {0}" -f $clause) -ForegroundColor Green
        $passed++
    } else {
        Write-Host ("FAIL   {0}" -f $clause) -ForegroundColor Red
        $failed++
    }
}

Write-Host ("-" * 70)
Write-Host ("Result: {0} passed, {1} failed" -f $passed, $failed)

# --- 3. Does the user match the whole filter? ------------------------------

$nestedFilter = [regex]::Replace(
    $UsersFilter,
    '(?i)\(memberof=([^()]*OU=DynamicGroups[^()]*)\)',
    "(memberOf:${NestedRuleOid}:=`$1)"
)
$wholeFilter = "(&$identity$nestedFilter)"
$matchesAll  = $null -ne (New-Searcher $wholeFilter).FindOne()

Write-Host ""
if ($matchesAll) {
    Write-Host "User MATCHES the full USERSFILTER." -ForegroundColor Green
} else {
    Write-Host "User does NOT match the full USERSFILTER." -ForegroundColor Red
}
