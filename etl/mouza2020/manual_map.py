# PBS tehsil code -> (boundary district or None to keep the mapped one,
#                     boundary tehsil name,
#                     'parent' | 'variant' | 'approx')
#
# 'variant'  the same place, spelled differently by PBS and geoBoundaries.
# 'parent'   a sub-tehsil created after the polygons were drawn, folded into the
#            unit it was carved out of. Geographically contained, so the sum is right.
# 'approx'   the boundary file has no polygon for this area and the tehsil had to
#            be placed in a neighbour. Sums stay correct at district level but the
#            tehsil-level placement is a judgement call. Flagged in the crosswalk.

MANUAL = {
    # --- Punjab -----------------------------------------------------------
    "0081": (None, "Liaqat Pur", "variant"),        # Liaquatpur (Cholistan)
    "081":  (None, "Liaqat Pur", "variant"),
    "602":  (None, "Bahawalpur", "parent"),         # Bahawalpur Saddar
    "086":  (None, "Dera Ghazi Khan", "parent"),    # Kot Chhutta
    "115":  (None, "Kamalia", "parent"),            # Pir Mahal, carved from Kamalia
    "118":  (None, "Gujranwala", "parent"),         # Gujranwala Saddar
    "134":  (None, "Pasrur", "variant"),            # Pasroor
    "156":  (None, "Karorpacca", "variant"),        # Kahror Pacca
    "167":  (None, "Hassanabdal", "variant"),       # Hasan Abdal
    "174":  (None, "Tala Gang", "parent"),          # Lawa, carved from Talagang
    "205":  (None, "Bhalwal", "approx"),            # Bhera
    "597":  (None, "Noorpur", "approx"),            # Nowshera, Soon valley
    "547":  (None, "Lahore City", "approx"),        # Model Town
    "548":  (None, "Lahore City", "approx"),        # Raiwind
    "549":  (None, "Lahore City", "approx"),        # Shalimar

    # --- Khyber Pakhtunkhwa and the merged districts ----------------------
    "637":  (None, "Fr Bannu", "parent"),           # Baka Khel
    "638":  (None, "Bannu", "parent"),              # Miryan
    "605":  (None, "Fr D.I.Khan", "parent"),        # Drazanda
    "606":  (None, "Fr Tank", "parent"),            # Jandola
    "617":  (None, "Abbottabad", "approx"),         # Lora
    "618":  (None, "Abbottabad", "approx"),         # Lower Tanawal
    "619":  (None, "Haripur", "parent"),            # Khanpur
    "654":  (None, "Dassu", "parent"),              # Harban Bhasha, Upper Kohistan
    "655":  (None, "Dassu", "parent"),              # Seo, Upper Kohistan
    "660":  (None, "Pattan", "parent"),             # Bankad, Lower Kohistan
    "661":  (None, "Palas", "parent"),              # Batera, Kolai Palas
    "615":  (None, "Oghi", "approx"),               # Darband
    "616":  (None, "Mansehra", "parent"),           # Baffa Pakhal
    "035":  (None, "Daggar", "approx"),             # Mandanr
    "629":  (None, "Chitral", "parent"),            # Drosh
    "630":  (None, "Mastuj", "parent"),             # Torkhow / Mulkhow
    "625":  (None, "Swat Ranizai", "parent"),       # Batkhela
    "626":  (None, "Sam Ranizai", "parent"),        # Dargai
    "627":  (None, "Swat Ranizai", "approx"),       # Thana Baizai
    "656":  (None, "Babuzai", "approx"),            # residual "Swat"
    "623":  (None, "Katlang", "approx"),            # Rustam Sudham
    "635":  (None, "Takht Bhai", "parent"),         # Garhi Kapura
    "546":  (None, "Nowshera", "approx"),           # Jehangira
    "608":  (None, "Fr Peshawar", "parent"),        # Hassan Khel
    "639":  (None, "Fr Peshawar", "approx"),        # Mattani
    "642":  (None, "Peshawar Iv", "approx"),        # Shah Alam
    "458":  (None, "Jamrud", "approx"),             # Mulla Gori
    "613":  (None, "Halimzai", "approx"),           # residual "Mohmand Agency"
    "482":  (None, "Birmal", "variant"),            # Birmil
    "485":  (None, "Saraogha", "variant"),          # Sararogha
    "469":  (None, "Data Khel", "variant"),         # Datta Khel
    "471":  (None, "Garyum", "variant"),            # Gharyum
    "448":  (None, "Bar Chamarkand", "variant"),
    "467":  (None, "Upper Momand", "variant"),      # Upper Mohmand
    "468":  (None, "Yaka Ghund", "variant"),        # Yake Ghund
    "039":  (None, "Lalqilla", "variant"),          # Lal Qila

    # --- Sindh ------------------------------------------------------------
    "212":  (None, "Shaheed Fazal Rahu", "variant"),  # Golarchi (S.F. Rahu)
    "225":  (None, "Manjand", "variant"),             # Manjhand
    "235":  (None, "Tando Gulam Hyder", "variant"),
    "261":  (None, "Sijawal Junejo", "variant"),      # Sujawal Junejo
    "550":  (None, "Mirpurkhas", "approx"),           # Shujaabad
    "552":  (None, "Nagar Parkar", "approx"),         # Dahli
    "553":  (None, "Mithi", "parent"),                # Islamkot, carved from Mithi
    "601":  (None, "Diplo", "parent"),                # Kaloi, carved from Diplo
    "580":  (None, "Gadap Town", "approx"),           # Manghopir
    "582":  (None, "Baldia Town", "approx"),          # Mauripur
    "570":  (None, "Bin Qasim Town", "parent"),       # Ibrahim Hydri
    "586":  (None, "Gadap Town", "parent"),           # Murad Memon
    "593":  (None, "Bin Qasim Town", "approx"),       # Shah Mureed

    # --- Balochistan ------------------------------------------------------
    "634":  (None, "Jhal Jhao", "parent"),          # Korak Jhao
    "327":  (None, "Karkah Sub", "variant"),        # Karakh
    "351":  (None, "Shahdo Garhi Sub", "variant"),  # Shaho Gardi
    "354":  (None, "Jiwani", "variant"),            # Chiwani
    "357":  (None, "Sunstar Sub", "variant"),       # Suntser
    "359":  (None, "Buleda", "variant"),            # Bulaida
    "378":  ("SIBI", "Bhag", "variant"),            # Bhag sits under Sibi in ADM3
    "614":  (None, "Dera_Murad_Jamali", "approx"),  # Landhi
    "390":  (None, "Naukandi", "variant"),          # Nokundi
    "394":  ("PISHIN", "Gulistan", "variant"),      # Gulistan sits under Pishin in ADM3
    "398":  (None, "Huramzai", "variant"),          # Haram Zai
    "405":  (None, "Bekerh", "variant"),            # Baikar
    "408":  (None, "Malum", "variant"),             # Malam
    "414":  (None, "Khost", "variant"),             # Khoast
    "416":  (None, "Girsani Sub", "variant"),       # Grisani
    "420":  (None, "Kohlu", "approx"),              # Tamboo, Kohlu
    "421":  (None, "Kut Mandai", "variant"),        # Kot Mandai
    "434":  (None, "Shinkai", "variant"),           # Shinki
    "437":  (None, "Loralai", "parent"),            # Bori
    "651":  (None, "Drug Sub", "approx"),           # Tear Essot
    "652":  (None, "Musa Khel", "approx"),          # Zamri Palasin
    "322":  ("KALAT", "Surab", "variant"),          # Sorab, Shaheed Sikandarabad
    "657":  ("KALAT", "Surab", "approx"),           # Dasht e Gora
}
