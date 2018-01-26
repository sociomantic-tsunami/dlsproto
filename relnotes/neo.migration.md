## Neo support removed

`dlsproto.client.DlsClient`

An early version of neo support was added in the DLS client in v4.1.0. Since
then, development of the neo functionality has been largely in the `neo` branch,
not in the main release branches. The neo support in v13.x.x is thus very
outdated. This release removes this code entirely. Users who wish to use the neo
protocol should use the `neo` branch of this repo, which will be merged into
v14.x.x when ready for a proper release.

