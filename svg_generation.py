import svgwrite
from constants import *

LABEL_FONT_SIZE = 34
INFO_FONT_SIZE = 26

BAR_HEIGHT = 70
BAR_GAP = 10



def generate_energy_rating_svg(energy_rating, start_time_string, end_time_string):

    colours = (svgwrite.rgb(0,140,88),
               svgwrite.rgb(32,172,41),
               svgwrite.rgb(173,213,0),
               svgwrite.rgb(247,247,0),
               svgwrite.rgb(246,189,0),
               svgwrite.rgb(230,99,19),
               svgwrite.rgb(221,0,32))

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

    svg_drawing.add(svg_drawing.text(start_time_string + " - " + end_time_string,
                                     insert=(200, 40),
                                     fill=svgwrite.rgb(0,0,0),
                                     stroke=svgwrite.rgb(0,0,0),
                                     stroke_width=1,
                                     font_size=INFO_FONT_SIZE,
                                     font_family="Helvetica"))
    CO2_text = svg_drawing.text("CO2 g/km",
                                     insert=(5, 40),
                                     fill=svgwrite.rgb(0,0,0),
                                     stroke=svgwrite.rgb(0,0,0),
                                     stroke_width=1,
                                     font_size=INFO_FONT_SIZE,
                                     font_family="Helvetica")
    svg_drawing.add(CO2_text)

    #svg_drawing.add.viewbox(minx=0, miny=0, width=170 + 10 * (len(colours) - 1))
    for i in range(len(colours)):
        #Draw the energy rating bars
        x_left = 100
        x_right = x_left + 200 + CO2_amounts[i] * 3
        x_arrow_tip = x_right + 30
        y_top = 80 + (BAR_HEIGHT + BAR_GAP) * i
        y_bottom = y_top + BAR_HEIGHT #height is 26 units + 5 units gap between bars
        y_middle = (y_top + y_bottom) / 2
        if distances[i] > 0.1:
            opacity = 1
        else:
            opacity = 0.4

        svg_drawing.add(svg_drawing.polygon(((x_left, y_top), (x_right, y_top), (x_arrow_tip, y_middle), (x_right, y_bottom), (x_left, y_bottom)),
                                            fill=colours[i],
                                            stroke=colours[i],
                                            fill_opacity=opacity,
                                            stroke_width=3))

        svg_drawing.add(svg_drawing.text(labels[i],
                                         insert=(x_left + 5, y_middle + LABEL_FONT_SIZE / 2),
                                         fill=svgwrite.rgb(50,50,50),
                                         stroke=svgwrite.rgb(50,50,50),
                                         stroke_width=2,
                                         font_size=LABEL_FONT_SIZE,
                                         font_family="Helvetica",
                                         font_weight="bold"))

        svg_drawing.add(svg_drawing.text("{:.1f} km".format(distances[i]),
                                         insert=(x_right, y_middle + LABEL_FONT_SIZE / 2),
                                         fill=svgwrite.rgb(50,50,50),
                                         stroke=svgwrite.rgb(50,50,50),
                                         stroke_width=2,
                                         font_size=LABEL_FONT_SIZE,
                                         font_family="Helvetica",
                                         font_weight="bold",
                                         text_anchor="end"))

        svg_drawing.add(svg_drawing.text(CO2_amounts[i],
                                         insert=(x_left - 25, y_middle + LABEL_FONT_SIZE / 2),
                                         fill=svgwrite.rgb(0,0,0),
                                         stroke=svgwrite.rgb(0,0,0),
                                         stroke_width=0.5,
                                         font_size=LABEL_FONT_SIZE,
                                         font_family="Helvetica",
                                         text_anchor="end"))



    bottom_bar_amounts = (0, 25, 50, 75, 100, 125, 150)

    BAR_MIN = 150
    BAR_MAX = 800
    BAR_Y = 700
    BAR_TICK_SIZE = 20
    ARROW_WIDTH = 20
    ARROW_HEIGHT = 40
    ARROW_X_MIDDLE = BAR_MIN + (BAR_MAX - BAR_MIN) * (energy_rating.average_co2 / bottom_bar_amounts[-1])

    STEP_LENGTH = (BAR_MAX - BAR_MIN) / (len(bottom_bar_amounts) - 1)

    svg_drawing.add(svg_drawing.line(start=(BAR_MIN, BAR_Y),
                                     end=(BAR_MAX, BAR_Y),
                                     stroke=svgwrite.rgb(0,0,0),
                                     stroke_width=2))

    for i in range(len(bottom_bar_amounts)):
        svg_drawing.add(svg_drawing.line(start=(i * STEP_LENGTH + BAR_MIN, BAR_Y + BAR_TICK_SIZE),
                                         end=(i * STEP_LENGTH + BAR_MIN, BAR_Y),
                                         stroke=svgwrite.rgb(0,0,0),
                                         stroke_width=2))

        svg_drawing.add(svg_drawing.text(bottom_bar_amounts[i],
                                 insert=(i * STEP_LENGTH + BAR_MIN, BAR_Y + BAR_TICK_SIZE + INFO_FONT_SIZE),
                                 fill=svgwrite.rgb(0,0,0),
                                 stroke=svgwrite.rgb(0,0,0),
                                 stroke_width=1,
                                 font_size=INFO_FONT_SIZE,
                                 font_family="Helvetica",
                                 text_anchor="middle"))

    svg_drawing.add(svg_drawing.text("Average:",
                                 insert=(10, BAR_Y),
                                 fill=svgwrite.rgb(0,0,0),
                                 stroke=svgwrite.rgb(0,0,0),
                                 stroke_width=1,
                                 font_size=INFO_FONT_SIZE,
                                 font_family="Helvetica"))

    svg_drawing.add(svg_drawing.polygon(((ARROW_X_MIDDLE, BAR_Y - 10), (ARROW_X_MIDDLE + ARROW_WIDTH, BAR_Y - ARROW_HEIGHT), (ARROW_X_MIDDLE - ARROW_WIDTH, BAR_Y - ARROW_HEIGHT)),
                                        fill="black",
                                        stroke="black",
                                        stroke_width=3))
    CO2_text = svg_drawing.text("CO2 g/km",
                                insert=(BAR_MAX + 40, BAR_Y + BAR_TICK_SIZE + INFO_FONT_SIZE),
                                fill=svgwrite.rgb(0,0,0),
                                stroke=svgwrite.rgb(0,0,0),
                                stroke_width=1,
                                font_size=INFO_FONT_SIZE,
                                font_family="Helvetica")
    svg_drawing.add(CO2_text)

    #Draw pointer
    #x_arrow_tip = (len(colours) - 1) * 10 * final_rating + 175
    #x_right = x_arrow_tip + 20
    #y_top = 5 + final_rating * (len(colours) - 1) * 30
    #y_bottom = y_top + 26
    #y_arrow_tip = (y_top + y_bottom) / 2
    #svg_drawing.add(svg_drawing.polygon(((x_arrow_tip, y_arrow_tip), (x_right, y_top), (x_right, y_bottom)), fill=svgwrite.rgb(0,0,0), stroke=svgwrite.rgb(0,0,0)))

    average_co2_string = "Average CO2 emission (g/km): {:.1f}".format(energy_rating.average_co2)
    total_co2_string = "Total CO2 emission (g): {:.1f}".format(energy_rating.total_co2)

    #svg_drawing.add(svg_drawing.text(average_co2_string, insert=(5, 800), fill=svgwrite.rgb(0,0,0), stroke=svgwrite.rgb(0,0,0), stroke_width=1, font_size=26, font_family="Helvetica"))
    #svg_drawing.add(svg_drawing.text(total_co2_string, insert=(5, 900), fill=svgwrite.rgb(0,0,0), stroke=svgwrite.rgb(0,0,0), stroke_width=1, font_size=26, font_family="Helvetica"))
    return svg_drawing.tostring()