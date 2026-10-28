<#
.SYNOPSIS
Exporta as figuras em HTML/SVG de texto/figuras para PDF (vetorial, usado no
texto) e PNG (so para conferir sem compilar), com o Edge ou o Chrome sem janela.

.DESCRIPTION
O tamanho de cada figura vem da regra @page do proprio HTML. As fontes vem do
Google Fonts, entao a exportacao precisa de rede.

.EXAMPLE
./texto/figuras/exportar.ps1
./texto/figuras/exportar.ps1 -Nomes modos
#>
param(
    [string[]]$Nomes = @('arquitetura', 'modos', 'segundo_cenario')
)

$here = $PSScriptRoot
$out = Split-Path $here -Parent
$browser = @(
    "${env:ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe",
    "$env:ProgramFiles\Google\Chrome\Application\chrome.exe"
) | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $browser) { throw 'Edge ou Chrome nao encontrado.' }

# Perfil temporario: nao toca no navegador aberto do usuario.
$profile = Join-Path $env:TEMP "pitwall-figuras-$PID"
New-Item -ItemType Directory -Force $profile | Out-Null

foreach ($nome in $Nomes) {
    $html = Join-Path $here "$nome.html"
    $size = [regex]::Match((Get-Content $html -Raw), '@page \{ size: (\d+)px (\d+)px')
    $w = [int]$size.Groups[1].Value
    $h = [int]$size.Groups[2].Value
    $url = 'file:///' + ($html -replace '\\', '/')
    $common = @('--headless=new', '--disable-gpu', "--user-data-dir=$profile", '--virtual-time-budget=8000', '--hide-scrollbars')

    $pdf = Join-Path $out "fig_$nome.pdf"
    & $browser @common '--no-pdf-header-footer' "--print-to-pdf=$pdf" $url 2>$null | Out-Null
    $png = Join-Path $out "fig_$nome.png"
    & $browser @common "--window-size=$w,$h" '--force-device-scale-factor=4' "--screenshot=$png" $url 2>$null | Out-Null

    Write-Host "escrito: fig_$nome.pdf e fig_$nome.png (${w}x${h})"
}

Remove-Item -Recurse -Force $profile -ErrorAction SilentlyContinue
