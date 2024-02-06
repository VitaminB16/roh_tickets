const zones = [
  { selector: '#seatmap #Amphitheatre .seat', name: 'Amphitheatre' },
  { selector: '#seatmap #Orchestra-Stalls .seat', name: 'Orchestra Stalls' },
  { selector: '#seatmap #Stalls-Circle .seat', name: 'Stalls Circle' },
  { selector: '#seatmap #Grand-Tier .seat', name: 'Donald Gordon Grand Tier' },
  { selector: '#seatmap #Balcony .seat', name: 'Balcony' },
];

let records = [];

zones.forEach(zone => {
  const seats = document.querySelectorAll(zone.selector);
  seats.forEach(seat => {
    let cx, cy;
    let seat_killed = seat.className.baseVal.includes("seat--killed")
    // Skip killed seats
    if (seat_killed) {
      return;
    }
    // Check if the seat is a <circle> element
    if ((seat.tagName.toLowerCase() === 'circle') || (seat.tagName.toLowerCase() === 'ellipse')) {
      cx = seat.getAttribute('cx');
      cy = seat.getAttribute('cy');
    }
    // If the seat is a <g> element, extract coordinates from the transform attribute
    else if (seat.tagName.toLowerCase() === 'g') {
      const transform = seat.getAttribute('transform');
      const match = /translate\(([^,]+),\s*([^)]+)\)/.exec(transform);
      if (match) {
        [cx, cy] = [match[1], match[2]];
      }
    }

    // Only add a record if we successfully extracted coordinates
    if (cx !== undefined && cy !== undefined) {
      const record = {
        cx,
        cy,
        ZoneName: zone.name,
        id: seat.id,
      };
      records.push(record);
    }
  });
});
