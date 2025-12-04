// Working with MongoDB's LLM

// ------------------------------------------------------------------------------------------------------------------------------------------------------------------------

// Part 1: Typical Research Ideas:

// ---------------------------------------
// Aircraft and Operator Safety Analysis:
// ---------------------------------------
// #1 Goal: Provide the top 10 aircraft models involved in the highest number of accidents?
// #1 Prompt: Provide the top 10 aircraft models involved in the highest number of accidents?
db.getCollection('airplane_crashes').aggregate(
    [
  {
    $group: {
      _id: "$AC Type",
      totalAccidents: {
        $sum: 1
      }
    }
  },
  {
    $sort: {
      totalAccidents: -1
    }
  },
  {
    $limit: 10
  }
]
);

// #2 Goal: Which commercial operators (airlines) have the highest average number of fatalities per crash?
// #2 Prompt: Which commercial operators (airlines) have the highest average number of fatalities per crash?
db.getCollection("airplane_crashes").aggregate([
  {
    $match: {
      Operator: { $ne: null }
    }
  },
  {
    $group: {
      _id: "$Operator",
      avgFatalities: { $avg: "$Fatalities" }
    }
  },
  {
    $sort: {
      avgFatalities: -1
    }
  }
]);

// # 3 Goal: For a specific aircraft type (e.g. Boeing 747), what is the ratio of fatalities to persons aboard across all incidents?
// #3 Prompt: Calculate the ratio of fatalities to people aboard across all incidents, grouped by each specific aircraft type.
db.getCollection('airplane_crashes').aggregate(
  [
    {
      $group: {
        _id: "$AC Type",
        totalFatalities: { $sum: "$Fatalities" },
        totalAboard: { $sum: "$Aboard" }
      }
    },
    {
      $project: {
        ratioFatalitiesToAboard: {
          $cond: {
            if: { $eq: ["$totalAboard", 0] },
            then: 0,
            else: { $divide: ["$totalFatalities", "$totalAboard"] }
          }
        }
      }
    }
  ]
);

// # 4 Goal: What percentage of total crashes involve military operators versus civil/commercial operators?
// #4 Prompt: What percentage of total crashes involve military operators versus civil/commercial operators?
db.getCollection('airplane_crashes').aggregate(
  [
    {
      $group: {
        _id: {
          $cond: {
            if: { $regexMatch: { input: "$Operator", regex: /military/i } },
            then: "Military",
            else: "Civil"
          }
        },
        count: { $sum: 1 }
      }
    },
    {
      $group: {
        _id: null,
        totalCrashes: { $sum: "$count" },
        crashesByType: {
          $push: {
            type: "$_id",
            count: "$count"
          }
        }
      }
    },
    {
      $project: {
        crashesByType: {
          $map: {
            input: "$crashesByType",
            as: "typeData",
            in: {
              type: "$$typeData.type",
              percentage: {
                $multiply: [
                  { $divide: ["$$typeData.count", "$totalCrashes"] }, 100] }
            }
          }
        }
      }
    }
  ]
);

/* 
Aircraft and Operator Safety questions reflection: 
---------------------------------------------------
It was quite easy to utilize the LLM when answering these questions. I did not run into any issues or errors. 
I did not tweak anything when importing the data, and MongoDB was smart enough to import all of the needed fields 
as the correct data type (no integers imported as strings, for example).
*/
// ------------------------------------


// Temporal Pattern Identification:
// ------------------------------------
// #1 Goal: How has the total number of fatalities per decade changed since the 1950s (looking for general improvement/deterioration)?
// #1 Prompt: Generate a MongoDB aggregation pipeline that starts with accidents from 1950 onward. The field "Date" is stored as a string in the format "MM/DD/YYYY". 
// Extract the year from the "Date" string with $substr and convert it to an integer, for example year. Filter out any records where year is less than 1950. 
// Compute the decade start as year minus (year modulo 10) and store it in a field like decadeStart. Group by decadeStart and sum the "Fatalities" field into totalFatalities. 
// Sort the grouped results by decadeStart in ascending order. Group again into a single document that contains two arrays: 
// one array of the sorted decadeStart values and one array of the corresponding totalFatalities values, using $group with $push. 
// In a $project stage, use build an array of documents where each element contains: the decade start, the total fatalities for that decade, and the difference from the previous decade. 
// For index 0, set the difference to null or 0. For later indices, subtract the previous decade total from the current decade total. 
// $unwind that array so that you return one document per decade. Output a field DecadeLabel like "1950s", along with TotalFatalities and DifferenceFromPrevious, and exclude _id.
db.getCollection('airplane_crashes').aggregate(
  [
    {
      $addFields: {
        year: {
          $toInt: {
            $substr: ["$Date", 6, 4]
          }
        }
      }
    },
    {
      $match: {
        year: { $gte: 1950 }
      }
    },
    {
      $addFields: {
        decadeStart: {
          $subtract: [
            "$year",
            { $mod: ["$year", 10] }
          ]
        }
      }
    },
    {
      $group: {
        _id: "$decadeStart",
        totalFatalities: { $sum: "$Fatalities" }
      }
    },
    {
      $sort: { _id: 1 }
    },
    {
      $group: {
        _id: null,
        decadeStarts: { $push: "$_id" },
        totalFatalitiesList: { $push: "$totalFatalities" }
      }
    },
    {
      $project: {
        combinedArray: {
          $map: {
            input: { $range: [0, { $size: "$decadeStarts" }] },
            as: "index",
            in: {
              decadeStart: { $arrayElemAt: ["$decadeStarts", "$$index"] },
              totalFatalities: { $arrayElemAt: ["$totalFatalitiesList", "$$index"] },
              difference: {
                $cond: {
                  if: { $eq: ["$$index", 0] },
                  then: null,
                  else: {
                    $subtract: [
                      { $arrayElemAt: ["$totalFatalitiesList", "$$index"] },
                      { $arrayElemAt: ["$totalFatalitiesList", { $subtract: ["$$index", 1] }] }
                    ]
                  }
                }
              }
            }
          }
        }
      }
    },
    {
      $unwind: "$combinedArray"
    },
    {
      $project: {
        DecadeLabel: {
          $concat: [
            { $toString: "$combinedArray.decadeStart" },
            "s"
          ]
        },
        TotalFatalities: "$combinedArray.totalFatalities",
        DifferenceFromPrevious: "$combinedArray.difference",
        _id: 0
      }
    }
  ]
);

// #2 Goal: Are accidents significantly more common during daylight (e.g. 06:00 to 18:00) or nighttime hours?
// #2 Prompt: Are accidents significantly more common during daylight (6am to 6pm) or nighttime hours?
db.getCollection("airplane_crashes").aggregate(
  [
    {
      $addFields: {
        time_parts: { $split: ["$Time", ":"] }
      }
    },
    {
      $project: {
        hour: { $arrayElemAt: ["$time_parts", 0] },
        Date: 1
      }
    },
    {
      $match: {
        hour: { $gte: "06", $lt: "18" }
      }
    },
    {
      $group: {
        _id: "daylight",
        count: { $sum: 1 }
      }
    },
    {
      $unionWith: {
        coll: "airplane_crashes",
        pipeline: [
          {
            $addFields: {
              time_parts: { $split: ["$Time", ":"] }
            }
          },
          {
            $project: {
              hour: { $arrayElemAt: ["$time_parts", 0] },
              Date: 1
            }
          },
          {
            $match: {
              $or: [
                { hour: { $lt: "06" } },
                { hour: { $gte: "18" } }
              ]
            }
          },
          {
            $group: {
              _id: "nighttime",
              count: { $sum: 1 }
            }
          }
        ]
      }
    }
  ]
);

// #3 Goal: Which single date has the highest combined total of onboard and ground fatalities?
// #3 Prompt: Which single date has the highest combined total of onboard and ground fatalities?
db.getCollection('airplane_crashes').aggregate(
  [
    {
      $group: {
        _id: "$Date",
        totalFatalities: {
          $sum: {
            $add: ["$Fatalities", "$Ground"]
          }
        }
      }
    },
    {
      $sort: { totalFatalities: -1 }
    },
    {
      $limit: 1
    }
  ]
);
/* 
Temporal Pattern Identification questions reflection: 
---------------------------------------------------
It was fairly easy to utilize the LLM when answering these questions. 
- I ran into issues with the first question, calculating the number of Fatalities by decade and looking for trends. 
----- I had to explicitly tell MongDB how to calculate the decades for the years, how to group data, and what the output should look like. 
----- Without doing this, MongoDB seemed to get a bit confused with how to extract the decade and do the calculations properly.
- For question 2, the only issue was the (06:00 to 18:00) portion of the original question. 
----- MongoDB was a bit confused by this syntax, so I changed it to say “6am to 6pm” in order for it to understand how to create a proper query.
- For question 3, I was able to use the question directly from the assignment in order to generate a query that solved the problem.
*/
// ------------------------------------


// Geographical Incident Analysis:
// ------------------------------------
// #1 Goal: Which specific cities, airports, or geographical areas have experienced the greatest number of incidents?
// #1 Prompt: Which specific cities, airports, or geographical areas have the most incidents?
db.getCollection('airplane_crashes').aggregate(
  [
    {
      $group: {
        _id: "$Location",
        count: { $sum: 1 }
      }
    },
    {
      $sort: { count: -1 }
    }
  ]
);

// #2 Goal: In what types of locations (e.g dense population vs remote area) do crashes result in the highest number of ground fatalities?
// #2 Prompt: In what types of locations (e.g dense population vs remote area) do crashes result in the highest number of ground fatalities?
db.getCollection('airplane_crashes').aggregate(
  [
    {
      $group: {
        _id: "$Location",
        totalGroundFatalities: { $sum: "$Ground" }
      }
    },
    {
      $sort: { totalGroundFatalities: -1 }
    },
    {
      $limit: 10
    }
  ]
);

// #3 Goal: How many incidents are recorded where the number of onboard fatalities was zero? (A measure of crash survivability).
// #3 Prompt: How many incidents are recorded where the number of onboard fatalities was zero?
db.getCollection('airplane_crashes').aggregate(
  [
    {
      $match: {
        Fatalities: 0
      }
    },
    {
      $count: "zeroFatalitiesIncidents"
    }
  ]
);
/* 
Geographical Incident Analysis questions reflection: 
---------------------------------------------------
For these questions, it was also quite easy to use the LLM to get meaningful answers.
- For question 1, my original prompt included the word “greatest” instead of “most”. 
----- Using the word “greatest” gave me a query with a limit of one at the end since the LLM was assuming I was only looking for the largest number. 
----- Since the goal mentions “cities, airports, and areas” (all plural), I did not want this limit in place, which is why I adjusted the wording of my prompt to get more results.
- For question 2, it seemed to be nearly impossible to have the query directly answer the question of dense populations versus remote areas. 
----- However, with the output that MongDB provided, we are clearly able to use our own reasoning to see what the top spots have in common. 
----- Clearly, dense population areas tend to have the most ground fatalities due to the large number of people in a given area.
- For question 3, using the goal as a prompt worked perfectly and gave an output that answers the question fully.
*/
// ------------------------------------


// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------



// Part 2

// Data Type Confusion Error:
// ---------------------------------------
// Initial Prompt: Calculate each Operator’s average fatality rate and sort from highest percentage to lowest. 
// Only output the Operator and the percentage.
db.getCollection('airplane_crashes2').aggregate(
  [
    {
      $project: {
        Operator: 1,
        Fatalities: { $toInt: '$Fatalities' },
        AboardTotal: {
          $sum: [
            { $toInt: '$Aboard Crew' },
            { $toInt: '$Aboard Passangers' }
          ]
        }
      }
    },
    {
      $group: {
        _id: '$Operator',
        TotalFatalities: { $sum: '$Fatalities' },
        TotalAboard: { $sum: '$AboardTotal' }
      }
    },
    {
      $project: {
        Operator: '$_id',
        FatalityRate: {
          $cond: {
            if: { $gt: ['$TotalAboard', 0] },
            then: {
              $multiply: [
                {
                  $divide: [
                    '$TotalFatalities',
                    '$TotalAboard'
                  ]
                },
                100
              ]
            },
            else: 0
          }
        }
      }
    },
    { $sort: { FatalityRate: -1 } },
    {
      $project: {
        _id: 0,
        Operator: 1,
        FatalityRate: 1
      }
    }
  ],
  { maxTimeMS: 60000, allowDiskUse: true }
);
/*
MongoDB used the sum of “Aboard Crew” and “Aboard Passengers” instead of using the “Aboard” field, which is stored as a string.
This output correctly attempted to change “Fatalities”, “Aboard Crew”, and “Aboard Passengers” to be integers using the $toInt command, but there was an error in doing so due to Null values: 
“Executor error during aggregate command on namespace: examples.airplane_crashes2 :: caused by :: Failed to parse number 'NULL' in $convert with no onError value: Did not consume whole string.” 
As a result, there was no output of this query.
In my next prompt, I tried telling MongoDB to directly divide Fatalities by Abroad in an attempt to have a true data type confusion error, hoping that MongoDB would not use the $toInt command.
*/

// Modified Prompt: Calculate each Operator’s average fatality rate by directly dividing Fatalities by Abroad, and sort from highest percentage to lowest. 
// Only output the Operator and the percentage.
db.getCollection('airplane_crashes2').aggregate(
  [
    {
      $addFields: {
        FatalitiesNumber: {
          $toInt: '$Fatalities'
        },
        AboardNumber: { $toInt: '$Aboard' }
      }
    },
    {
      $group: {
        _id: '$Operator',
        avgFatalityRate: {
          $avg: {
            $divide: [
              '$FatalitiesNumber',
              '$AboardNumber'
            ]
          }
        }
      }
    },
    {
      $project: {
        _id: 0,
        Operator: '$_id',
        avgFatalityRatePercentage: {
          $multiply: ['$avgFatalityRate', 100]
        }
      }
    },
    { $sort: { avgFatalityRatePercentage: -1 } }
  ],
  { maxTimeMS: 60000, allowDiskUse: true }
);
/*
This output used Fatalities and Abroad as part of the calculation, but MongoDB still attempted to convert them to integers.
The same error occurred, however, due to null values: 
“Executor error during aggregate command on namespace: examples.airplane_crashes2 :: caused by :: Failed to parse number 'NULL' in $convert with no onError value: Did not consume whole string.”
In summary, I was unable to create a Data Type Confusion error, as MongoDB always knew to convert to integers before doing mathematical operations.
*/
// ---------------------------------------


// Data Type Confusion Error:
// ---------------------------------------
// Ambiguous Context Prompt: Where do most planes that crash take off from?
db.getCollection("airplane_crashes2").aggregate(
  [
    {
      $project: {
        Region: {
          $arrayElemAt: [
            { $split: ["$Location", ", "] },
            -1
          ]
        }
      }
    },
    {
      $group: {
        _id: "$Region",
        CrashCount: { $sum: 1 }
      }
    },
    {
      $sort: { CrashCount: -1 }
    },
    {
      $limit: 5
    }
  ]
);
/*
The goal was to confuse MongoDB by asking for where the planes that crashed took off from, but also giving MongoDB the flexibility to pick the field that it thinks makes the most sense.
Location tells us where the plane crashed, but the Route field is needed to determine where the plane took off. 
In the query above, MongoDB mistakenly uses the Location field instead of the Route field:
*/

// Working Prompt: 
// Find the top 10 departure locations for flights that crashed. Use the Route field as the flight path. 
// Route is a string that often looks like "City A - City B" or "City A - City B - City C", but it sometimes has tab characters and inconsistent spacing, 
// and some values are junk like "Training flight" with no dash at all. 
// Treat only the records whose Route actually contains a dash as valid flight paths and ignore the rest. 
// First clean the text by removing tab characters and collapsing multiple spaces into a single space, and trim any leading or trailing whitespace. 
// Then normalize the separator so every path uses a single " - " between segments. 
// After that, treat the substring before the first " - " as the origin location of the flight. 
// Count how many crashes originated from each origin, sort from the highest count to the lowest, and return only the top 10. 
// Each result document should contain just the origin name and the number of crashes from that origin.
db.getCollection("airplane_crashes2").aggregate(
  [
    {
      $match: {
        Route: { $regex: "-" }
      }
    },
    {
      $addFields: {
        cleanRoute: {
          $trim: {
            input: {
              $reduce: {
                input: {
                  $split: [
                    {
                      $replaceAll: {
                        input: "$Route",
                        find: "\t",
                        replacement: " "
                      }
                    },
                    " "
                  ]
                },
                initialValue: "",
                in: {
                  $cond: [
                    { $eq: ["$$this", ""] },
                    "$$value",
                    {
                      $concat: [
                        "$$value",
                        {
                          $cond: [
                            { $eq: ["$$value", ""] },
                            "",
                            " "
                          ]
                        },
                        "$$this"
                      ]
                    }
                  ]
                }
              }
            }
          }
        }
      }
    },
    {
      $addFields: {
        origin: {
          $trim: {
            input: {
              $arrayElemAt: [
                { $split: ["$cleanRoute", " - "] },
                0
              ]
            }
          }
        }
      }
    },
    {
      $group: {
        _id: "$origin",
        count: { $sum: 1 }
      }
    },
    {
      $sort: { count: -1 }
    },
    {
      $limit: 10
    },
    {
      $project: {
        origin: "$_id",
        count: 1,
        _id: 0
      }
    }
  ]
);
/*
This query correctly uses the Route field, with direction from me. 
Data cleaning is performed as well, and I helped MongoDB understand how to know where each flight took off from. 
The output of the query helps us see that most planes that crash take off from Paris, London, Moscow, and New York City.
*/
// ---------------------------------------


// Complex Aggregation Structure:
// ---------------------------------------
// Complex Aggregation Structure Prompt: 
// Treat the Fatalities field as a numeric value but be aware that some rows have it as "NULL" or null, which should be ignored. 
// Use only the rows with real numeric Fatalities. I am interested in Operators that are associated with very severe crashes, 
// where the fatalities reach at least 500 overall, and I want to know how many such Operators there are. 
// Concentrate your calculation on the crashes that meet that level of severity and return a single number.
db.getCollection("airplane_crashes2").aggregate(
  [
    {
      $match: {
        $expr: {
          $and: [
            {
              $gt: [
                { $toInt: "$Fatalities" },
                499
              ]
            },
            { $ne: ["$Fatalities", null] },
            { $ne: ["$Fatalities", "NULL"] }
          ]
        }
      }
    },
    {
      $group: {
        _id: "$Operator",
        count: { $sum: 1 }
      }
    },
    {
      $count: "severeCrashOperators"
    }
  ]
);
/*
The goal of my request was to count “high-fatality Operators” whose total Fatalities reach at least 500 across all of their crashes. 
To solve this correctly, MongoDB would need to sum Fatalities per Operator and then filter based on that total.
The Fatalities field is stored as text, and some records contain “NULL” instead of a numeric value. 
My prompt warned MongoDB to avoid using those values and only treat real numeric Fatalities as valid data.
The query that MongoDB generated applied $toInt to every Fatalities values before checking whether values were null or “NULL”. 
As a result it attempted to concert these null values into a number, and when it did so, an error was triggered:
“Executor error during aggregate command on namespace: examples.airplane_crashes2 :: caused by :: Failed to parse number 'NULL' in $convert with no onError value: Did not consume whole string.”
In addition to causing this conversion failure, the query incorrectly filtered rows using Fatalities using “Fatalities > 499” before grouping. 
This only identifies operators with a single crash over 499 fatalities, rather than operators whose fatalities total at least 500 across multiple crashes.
Because of these incorrect aspects, the query does not produce an output, and even if it did run successfully, the query would still be logically incorrect.
*/

// Working Prompt:
// Treat the Fatalities field as a numeric value, but some rows contain the literal text “NULL” or are actually null, so those should be ignored when working with Fatalities. 
// Use only rows where Fatalities is a real number in text form. I’m interested in Operators linked to very severe crashes, where the total fatalities reach at least 500 overall. 
// Focus only on those severe cases when determining the Operators, and return just one number that shows how many of these Operators there are.
db.getCollection("airplane_crashes2").aggregate(
  [
    {
      $match: {
        Fatalities: { $regex: "^[0-9]+$" }
      }
    },
    {
      $addFields: {
        FatalitiesNumber: { $toInt: "$Fatalities" }
      }
    },
    {
      $group: {
        _id: "$Operator",
        TotalFatalities: { $sum: "$FatalitiesNumber" }
      }
    },
    {
      $match: {
        TotalFatalities: { $gte: 500 }
      }
    },
    {
      $count: "SevereOperatorsCount"
    }
  ]
);
/*
This prompt works well because it subtly gives more clarity about the process of what must happen in the query in order to get a proper answer.
The query allows us to see that there are 31 instances of Operators that go beyond this threshold.
*/
