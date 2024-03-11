const zones = [
  { selector: '#seatmap #Amphitheatre .seat', name: 'Amphitheatre' },
  { selector: '#seatmap #Orchestra-Stalls .seat', name: 'Orchestra Stalls' },
  { selector: '#seatmap #Stalls-Circle .seat', name: 'Stalls Circle' },
  { selector: '#seatmap #Grand-Tier .seat', name: 'Donald Gordon Grand Tier' },
  { selector: '#seatmap #Balcony .seat', name: 'Balcony' },
];
const text = [
  { selector: 'text.st3.st4.st6', type: 'Text' },
  { selector: 'text.st3.st4.st5', type: 'Text' },
]

let records = [];

// Process the seats information on the page
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

// Process the text information on the page
text.forEach(textInfo => {
  const textElements = document.querySelectorAll(textInfo.selector);
  textElements.forEach(textEl => {
    let cx, cy, textCont;
    const transform = textEl.getAttribute('transform');
    const matrixMatch = /matrix\(\d\s*\d\s*\d\s*\d\s*([-\d.]+)\s*([-\d.]+)\)/.exec(transform);
    if (matrixMatch) {
      [cx, cy] = [matrixMatch[1], matrixMatch[2]];
    }
    textCont = textEl.textContent

    // Ensure that the text contains at least one letter
    if (cx !== undefined && cy !== undefined && /[a-zA-Z]/.test(textCont)) {
      const record = {
        cx,
        cy,
        ZoneName: textCont,
        id: textInfo.type,
      };
      records.push(record);
    }
  });
});