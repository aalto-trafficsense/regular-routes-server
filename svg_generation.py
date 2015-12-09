import svgwrite
from datetime import timedelta
from constants import *


LABEL_FONT_SIZE = 34
INFO_FONT_SIZE = 26

BAR_HEIGHT = 70
BAR_GAP = 10


bottom_bar_amounts = (0, 25, 50, 75, 100, 125, 150)

BAR_MIN = 100
BAR_MAX = 800
BAR_Y = 700
BAR_TICK_SIZE = 20
ARROW_WIDTH = 20
ARROW_HEIGHT = 40

STEP_LENGTH = (BAR_MAX - BAR_MIN) / float(len(bottom_bar_amounts) - 1)


def generate_energy_rating_svg(energy_rating, start_time, end_time, ranking, ranking_max):

    ARROW_X_MIDDLE = BAR_MIN + (BAR_MAX - BAR_MIN) * (energy_rating.average_co2 / bottom_bar_amounts[-1])

    # end time is the midnight of a new day not yet in the rating, so show the previous date for the user
    end_time_string = (end_time - timedelta(days=1)).strftime("%Y-%m-%d")
    start_time_string = start_time.strftime("%Y-%m-%d")

    colours = [svgwrite.rgb(0,140,88),
               svgwrite.rgb(32,172,41),
               svgwrite.rgb(173,213,0),
               svgwrite.rgb(247,247,0),
               svgwrite.rgb(246,189,0),
               svgwrite.rgb(230,99,19),
               svgwrite.rgb(221,0,32)]

    labels = ("Bicycle",
              "Walking",
              "Running",
              "Mass transit A",
              "Mass transit B",
              "Mass transit C",
              "Car")

    CO2_amounts = (ON_BICYCLE_CO2,
                   WALKING_CO2,
                   RUNNING_CO2,
                   MASS_TRANSIT_A_CO2,
                   MASS_TRANSIT_B_CO2,
                   MASS_TRANSIT_C_CO2,
                   IN_VEHICLE_CO2)

    distances = (energy_rating.on_bicycle_distance,
                 energy_rating.walking_distance,
                 energy_rating.running_distance,
                 energy_rating.in_mass_transit_A_distance,
                 energy_rating.in_mass_transit_B_distance,
                 energy_rating.in_mass_transit_C_distance,
                 energy_rating.in_vehicle_distance)


    svg_drawing = svgwrite.Drawing(profile="full")
    svg_drawing.fit(horiz="left", vert="top")
    svg_drawing.viewbox(0,0, 1000, 800)

    #Dates
    svg_drawing.add(svg_drawing.text(start_time_string + " - " + end_time_string,
                                     insert=(200, 40),
                                     fill=svgwrite.rgb(0,0,0),
                                     stroke=svgwrite.rgb(0,0,0),
                                     stroke_width=1,
                                     font_size=INFO_FONT_SIZE,
                                     font_family="Helvetica"))
    #Average CO2
    svg_drawing.add(svg_drawing.text("{:.1f} CO2 g/km".format(energy_rating.average_co2),
                                     insert=(420, 160),
                                     fill=svgwrite.rgb(0,0,0),
                                     stroke=svgwrite.rgb(0,0,0),
                                     stroke_width=1,
                                     font_size=70,
                                     font_family="Helvetica"))

    #Ranking
    svg_drawing.add(svg_drawing.text("Ranking: {0} / {1}".format(ranking, ranking_max),
                                     insert=(420, 250),
                                     fill=svgwrite.rgb(0,0,0),
                                     stroke=svgwrite.rgb(0,0,0),
                                     stroke_width=1,
                                     font_size=70,
                                     font_family="Helvetica"))
    #Vertical km label
    svg_drawing.add(svg_drawing.text("km",
                                     insert=(30, 65),
                                     fill=svgwrite.rgb(0,0,0),
                                     stroke=svgwrite.rgb(0,0,0),
                                     stroke_width=1,
                                     font_size=LABEL_FONT_SIZE,
                                     font_family="Helvetica"))

    for i in range(len(colours)):
        #Draw the energy rating bars
        x_left = BAR_MIN
        # The max possible value should be BAR_MAX. In vehicle produces the maximum CO2
        x_right = x_left + CO2_amounts[i] * (BAR_MAX - BAR_MIN) / IN_VEHICLE_CO2
        x_arrow_tip = x_right + 30
        y_top = 80 + (BAR_HEIGHT + BAR_GAP) * i
        y_bottom = y_top + BAR_HEIGHT #height is 26 units + 5 units gap between bars
        y_middle = (y_top + y_bottom) / 2
        if distances[i] > 0.1:
            opacity = 1
        else:
            opacity = 0.4
            colours[i] = svgwrite.rgb(50,50,50)

        #Coloured or grey rating bars
        svg_drawing.add(svg_drawing.polygon(((x_left, y_top), (x_right, y_top), (x_arrow_tip, y_middle), (x_right, y_bottom), (x_left, y_bottom)),
                                            fill=colours[i],
                                            stroke=colours[i],
                                            fill_opacity=opacity,
                                            stroke_width=3))
        #Rating bar text
        svg_drawing.add(svg_drawing.text(labels[i],
                                         insert=(x_left + 5, y_middle - 5 + LABEL_FONT_SIZE / 2),
                                         fill=svgwrite.rgb(50,50,50),
                                         stroke=svgwrite.rgb(50,50,50),
                                         stroke_width=2,
                                         font_size=LABEL_FONT_SIZE,
                                         font_family="Helvetica"))
        #Rating bar distance
        svg_drawing.add(svg_drawing.text("{:.1f}".format(distances[i]),
                                         insert=(x_left - 5, y_middle + LABEL_FONT_SIZE / 2),
                                         fill=svgwrite.rgb(50,50,50),
                                         stroke=svgwrite.rgb(50,50,50),
                                         stroke_width=2,
                                         font_size=LABEL_FONT_SIZE,
                                         font_family="Helvetica",
                                         text_anchor="end"))

    #Bottom bar
    svg_drawing.add(svg_drawing.line(start=(BAR_MIN, BAR_Y),
                                     end=(BAR_MAX, BAR_Y),
                                     stroke=svgwrite.rgb(0,0,0),
                                     stroke_width=2))

    for i in range(len(bottom_bar_amounts)):
        #Bottom bar ticks
        svg_drawing.add(svg_drawing.line(start=(i * STEP_LENGTH + BAR_MIN, BAR_Y + BAR_TICK_SIZE),
                                         end=(i * STEP_LENGTH + BAR_MIN, BAR_Y),
                                         stroke=svgwrite.rgb(0,0,0),
                                         stroke_width=2))
        #Bottom bar co2 amounts
        svg_drawing.add(svg_drawing.text(bottom_bar_amounts[i],
                                 insert=(i * STEP_LENGTH + BAR_MIN, BAR_Y + BAR_TICK_SIZE + INFO_FONT_SIZE),
                                 fill=svgwrite.rgb(0,0,0),
                                 stroke=svgwrite.rgb(0,0,0),
                                 stroke_width=1,
                                 font_size=INFO_FONT_SIZE,
                                 font_family="Helvetica",
                                 text_anchor="middle"))
    #Bottom bar average text
    svg_drawing.add(svg_drawing.text("Average:",
                                 insert=(10, BAR_Y - 30),
                                 fill=svgwrite.rgb(0,0,0),
                                 stroke=svgwrite.rgb(0,0,0),
                                 stroke_width=1,
                                 font_size=INFO_FONT_SIZE,
                                 font_family="Helvetica"))
    #Bottom bar arrow
    svg_drawing.add(svg_drawing.polygon(((ARROW_X_MIDDLE, BAR_Y - 10), (ARROW_X_MIDDLE + ARROW_WIDTH, BAR_Y - ARROW_HEIGHT), (ARROW_X_MIDDLE - ARROW_WIDTH, BAR_Y - ARROW_HEIGHT)),
                                        fill="black",
                                        stroke="black",
                                        stroke_width=3))
    #Bottom bar unit (CO2 g/km)
    CO2_text = svg_drawing.text("CO2 g/km",
                                insert=(BAR_MAX + 40, BAR_Y + BAR_TICK_SIZE + INFO_FONT_SIZE),
                                fill=svgwrite.rgb(0,0,0),
                                stroke=svgwrite.rgb(0,0,0),
                                stroke_width=1,
                                font_size=INFO_FONT_SIZE,
                                font_family="Helvetica")
    svg_drawing.add(CO2_text)


    return svg_drawing.tostring()