// This file is used to detect the user's screen width and set the screen_width variable accordingly
window.dash_clientside = Object.assign({}, window.dash_clientside, {
  clientside: {
    detectScreenWidth: function (n_intervals) {
      return { screen_width: window.innerWidth };
    }
  }
});