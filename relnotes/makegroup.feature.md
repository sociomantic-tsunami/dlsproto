### Add helper method to create GroupRequest

* `dlsproto.client.legacy.internal.helper.GroupRequest`

  Previously, to instantiate the GroupRequest, the user had to pass the request
  struct as a template parameter, since the constructor wouldn't deduce it.
  Now the helper method `makeGroupRequest` is added which instantiates and
  returns the right `GroupRequest` instance without the need for the user to
  specify the template parameters. This is especially important when using the
  DMD 2.071.x where accessing the private request structs inside DlsClient
  is deprecated.

