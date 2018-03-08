### GetRange notification types started, suspended and resumed are removed

Since the GetRange v1, the request is immediately ready to be suspended,
and it also suspends and resumes instantly, without waiting for the nodes.
This makes `started`, `suspended` and `resumed` notification types useless
and they are now removed. The code inside the `started` notification should be
moved to be executed after creating the request, and the code for suspend/resume
should be removed and it should be assumed that the client will suspend/resume
immediately.
