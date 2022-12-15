CREATE TABLE "public.instrument" (
	"id" serial NOT NULL,
	"instr_type" int NOT NULL,
	"manufacturer" int NOT NULL,
	"material" int NOT NULL,
	"location" int NOT NULL,
	"status" int NOT NULL,
	"price" int NOT NULL,
	CONSTRAINT "instrument_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.status" (
	"status_id" int NOT NULL,
	"status_name" TEXT NOT NULL
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.location" (
	"location_id" int NOT NULL,
	"address" TEXT NOT NULL
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.material" (
	"material_id" int NOT NULL,
	"material_name" TEXT NOT NULL
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.guitar" (
	"id" int NOT NULL,
	"string_num" int NOT NULL,
	"fret_num" int NOT NULL,
	"floyd_rose" bit NOT NULL,
	"shape" int NOT NULL
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.shape" (
	"shape_id" int NOT NULL,
	"shape_name" TEXT NOT NULL
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.keyboard" (
	"id" int NOT NULL,
	"keynum" int NOT NULL,
	"is_synth" bit NOT NULL
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.drum" (
	"id" int NOT NULL,
	"diameter" int NOT NULL
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.employees" (
	"employee_id" serial NOT NULL,
	"employee_name" TEXT NOT NULL,
	CONSTRAINT "employees_pk" PRIMARY KEY ("employee_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.order_history" (
	"order_id" serial NOT NULL,
	"date" DATE NOT NULL,
	"location_id" int NOT NULL,
	"cashier_id" int NOT NULL,
	CONSTRAINT "order_history_pk" PRIMARY KEY ("order_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.manufacturer" (
	"manufacturer_id" serial NOT NULL,
	"manufacturer_name" varchar(255) NOT NULL,
	CONSTRAINT "manufacturer_pk" PRIMARY KEY ("manufacturer_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.orders_instruments" (
	"order_id" bigint NOT NULL,
	"instrument_id" int NOT NULL
) WITH (
  OIDS=FALSE
);



ALTER TABLE "instrument" ADD CONSTRAINT "instrument_fk0" FOREIGN KEY ("manufacturer") REFERENCES "manufacturer"("manufacturer_id");
ALTER TABLE "instrument" ADD CONSTRAINT "instrument_fk1" FOREIGN KEY ("material") REFERENCES "material"("material_id");
ALTER TABLE "instrument" ADD CONSTRAINT "instrument_fk2" FOREIGN KEY ("location") REFERENCES "location"("location_id");
ALTER TABLE "instrument" ADD CONSTRAINT "instrument_fk3" FOREIGN KEY ("status") REFERENCES "status"("status_id");




ALTER TABLE "guitar" ADD CONSTRAINT "guitar_fk0" FOREIGN KEY ("id") REFERENCES "instrument"("id");
ALTER TABLE "guitar" ADD CONSTRAINT "guitar_fk1" FOREIGN KEY ("shape") REFERENCES "shape"("shape_id");


ALTER TABLE "keyboard" ADD CONSTRAINT "keyboard_fk0" FOREIGN KEY ("id") REFERENCES "instrument"("id");

ALTER TABLE "drum" ADD CONSTRAINT "drum_fk0" FOREIGN KEY ("id") REFERENCES "instrument"("id");


ALTER TABLE "order_history" ADD CONSTRAINT "order_history_fk0" FOREIGN KEY ("location_id") REFERENCES "location"("location_id");
ALTER TABLE "order_history" ADD CONSTRAINT "order_history_fk1" FOREIGN KEY ("cashier_id") REFERENCES "employees"("employee_id");


ALTER TABLE "orders_instruments" ADD CONSTRAINT "orders_instruments_fk0" FOREIGN KEY ("order_id") REFERENCES "order_history"("order_id");
ALTER TABLE "orders_instruments" ADD CONSTRAINT "orders_instruments_fk1" FOREIGN KEY ("instrument_id") REFERENCES "instrument"("id");













