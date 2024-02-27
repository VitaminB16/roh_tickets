window.dash_clientside = Object.assign({}, window.dash_clientside, {
  clientside: {
    detectTheme: function (n_intervals) {
      return { dark_mode: window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches };
    }
  }
});