USE [MMB_P53]
GO
/****** Object:  Table [dbo].[AA_change]    Script Date: 26/10/2020 11:45:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[AA_change](
	[AAchange_ID] [smallint] IDENTITY(1,1) NOT NULL,
	[AAchange] [nvarchar](50) NOT NULL,
	[ProtDescription] [nvarchar](50) NOT NULL,
	[Codon_Number] [smallint] NOT NULL,
	[Mutant_AA] [nvarchar](3) NOT NULL,
	[Mut_rateAA] [decimal](18, 4) NULL,
	[GD] [decimal](18, 4) NULL,
	[AGVGDClass_2006] [nvarchar](50) NOT NULL,
	[AGVGDClass] [nvarchar](50) NOT NULL,
	[SIFTClass_2006] [nvarchar](50) NOT NULL,
	[SIFTClass] [nvarchar](50) NOT NULL,
	[Polyphen2] [nvarchar](50) NOT NULL,
	[REVEL] [decimal](18, 4) NULL,
	[BayesDel] [decimal](18, 4) NULL,
	[TransactivationClass] [nvarchar](50) NOT NULL,
	[DNE_LOFclass] [nvarchar](50) NOT NULL,
	[DNEclass] [nvarchar](50) NOT NULL,
	[StructureFunctionClass] [nvarchar](50) NOT NULL,
	[SwissProtLink] [nvarchar](10) NULL,
	[Complex] [nvarchar](3) NOT NULL,
 CONSTRAINT [PK_AAchange_ID] PRIMARY KEY CLUSTERED 
(
	[AAchange_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY],
 CONSTRAINT [U_AA_change_AAchange] UNIQUE NONCLUSTERED 
(
	[AAchange] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[AA_change] ADD  CONSTRAINT [DF_AA_change_Complex]  DEFAULT ('No') FOR [Complex]
GO
ALTER TABLE [dbo].[AA_change]  WITH NOCHECK ADD  CONSTRAINT [CK_AAchange_Complex] CHECK  (([Complex]='Yes' OR ([Complex]='No' OR [Complex]='NA')))
GO
ALTER TABLE [dbo].[AA_change] CHECK CONSTRAINT [CK_AAchange_Complex]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'A-GVGD predictions: 1=neutral, 0=deleterious, 2=Unclassified, 9=NA' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'AA_change', @level2type=N'COLUMN',@level2name=N'AGVGDClass_2006'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'A-GVGD predictions: 1=neutral, 0=deleterious, 2=Unclassified, 9=NA' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'AA_change', @level2type=N'COLUMN',@level2name=N'AGVGDClass'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'SIFT predictions: 1=neutral, 0=deleterious, 9=NA' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'AA_change', @level2type=N'COLUMN',@level2name=N'SIFTClass_2006'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'SIFT predictions: 1=neutral, 0=deleterious, 9=NA' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'AA_change', @level2type=N'COLUMN',@level2name=N'SIFTClass'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Yeast data Ishioka (all promoters): 1=functional (>45% and <=200% of WT), 0=non-functional (<=45% WT),  3=complex, 9=NA' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'AA_change', @level2type=N'COLUMN',@level2name=N'TransactivationClass'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Delaunay tesselation prediction with consistent model: 1=functional, 0=non-functional, 9=NA' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'AA_change', @level2type=N'COLUMN',@level2name=N'StructureFunctionClass'
GO
